"""
Video Upload API Routes

Provides endpoints for the video upload pipeline:
- GET /presigned_upload_url: Generate a presigned PUT URL for direct-to-B2 upload
- POST /video/status: Update video upload status (called after upload completes)
- GET /video/<video_key>/url: Get the public URL for a processed video
"""

import asyncio
import os
import uuid
from datetime import datetime, timedelta
from typing import Optional

from quart import Blueprint, jsonify, request
from quart_rate_limiter import rate_limit
from sqlalchemy.sql import text

from api.core import logger, get_db_session, metrics, reset_db_connections
from db import Player
from db.models import user_group_association
from db.models.video_upload import VideoUpload
from utils.b2_storage import (
    generate_presigned_upload_url,
)
from utils.video_storage import (
    VIDEO_STORAGE_BACKEND_DEFAULT,
    backend_for_video_record,
    build_raw_key,
    get_public_video_url,
    normalize_backend,
    object_exists,
    resolve_internal_path,
)

video_bp = Blueprint("video", __name__)

# Daily video limits from environment (with sensible defaults)
VIDEO_DAILY_LIMIT_FREE = int(os.getenv("VIDEO_DAILY_LIMIT_FREE", "50"))
VIDEO_DAILY_LIMIT_PREMIUM = int(os.getenv("VIDEO_DAILY_LIMIT_PREMIUM", "100"))
# Hard cap for local test upload payloads to protect API memory/disk.
VIDEO_LOCAL_MAX_UPLOAD_BYTES = max(
    1 * 1024 * 1024,
    int(os.getenv("VIDEO_LOCAL_MAX_UPLOAD_BYTES", str(32 * 1024 * 1024))),
)


def _get_daily_upload_count_for_group(group_id: int, db_session) -> int:
    """
    Count today's video uploads for a group.

    Uses a single aggregate query to avoid N+1 lookups across group members.
    """
    from sqlalchemy import and_, func

    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    count = (
        db_session.query(func.count(func.distinct(VideoUpload.id)))
        .join(
            user_group_association,
            user_group_association.c.player_id == VideoUpload.player_id,
        )
        .filter(
            and_(
                user_group_association.c.group_id == group_id,
                VideoUpload.created_at >= today_start,
            )
        )
        .scalar()
    )
    return int(count or 0)

def _normalize_fps(value) -> int:
    """Validate and normalize FPS value."""
    try:
        fps = int(value)
        if fps < 1 or fps > 60:
            return 20
        return fps
    except (ValueError, TypeError):
        return 20


def _get_premium_group_id_for_player(player_id: int, db_session) -> Optional[int]:
    """
    Resolve one premium group for a player in a single SQL round-trip.

    This replaces the previous N+1 flow that loaded all player groups first and
    then checked each group's premium upgrade status individually.
    """
    row = db_session.execute(
        text(
            "SELECT uga.group_id "
            "FROM user_group_association uga "
            "JOIN xenforo.xf_dt_group_upgrade_active xf ON xf.group_id = uga.group_id "
            "WHERE uga.player_id = :player_id "
            "AND xf.is_cancelled = 0 "
            "AND xf.group_upgrade_id > 2 "
            "ORDER BY uga.group_id ASC "
            "LIMIT 1"
        ),
        {"player_id": player_id},
    ).first()
    if not row:
        return None
    return int(row[0])


def _get_active_video_backend() -> str:
    """
    Return the configured upload backend for this process.

    This keeps local/B2 behavior runtime-configurable via env settings rather
    than a hardcoded module-level flag.
    """
    return normalize_backend(VIDEO_STORAGE_BACKEND_DEFAULT)


async def _resolve_upload_context(db_session, acc_hash: str):
    """
    Resolve player, premium group, and current quota usage.

    Returns:
        (player, premium_group, daily_limit, today_count, error_response)
    """
    if not acc_hash:
        return None, None, None, None, (jsonify({"error": "Missing acc_hash parameter"}), 400)

    player = await asyncio.to_thread(
        lambda: db_session.query(Player).filter(Player.account_hash == acc_hash).first()
    )
    if not player:
        return None, None, None, None, (jsonify({"error": "Player not found"}), 404)

    premium_group_id = await asyncio.to_thread(
        lambda: _get_premium_group_id_for_player(player.player_id, db_session)
    )
    if not premium_group_id:
        return None, None, None, None, (
            jsonify(
                {"error": "Player is not a member of a group who has subscribed for access to video uploads."}
            ),
            402,
        )

    # Membership query already validates active premium status, so this is fixed.
    daily_limit = VIDEO_DAILY_LIMIT_PREMIUM
    today_count = await asyncio.to_thread(
        lambda: _get_daily_upload_count_for_group(premium_group_id, db_session)
    )

    if today_count >= daily_limit:
        return None, None, None, None, (
            jsonify(
                {
                    "error": "Daily video limit reached",
                    "message": (
                        f"Your group has used {today_count}/{daily_limit} video uploads today. "
                        "Using a screenshot."
                    ),
                    "screenshot_only": True,
                    "daily_limit": daily_limit,
                    "daily_used": today_count,
                }
            ),
            402,
        )

    return player, premium_group_id, daily_limit, today_count, None


async def _create_upload_ticket(db_session, player_id: int, fps: int, storage_backend: str) -> VideoUpload:
    """Create a pending VideoUpload record and return it."""
    video_uuid = str(uuid.uuid4())
    object_key = build_raw_key(player_id=player_id, video_uuid=video_uuid, fps=fps)
    video_record = VideoUpload(
        player_id=player_id,
        video_key=object_key,
        fps=fps,
        status="pending",
        storage_backend=storage_backend,
    )
    db_session.add(video_record)
    await asyncio.to_thread(db_session.commit)
    return video_record


async def _issue_upload_ticket_b2(db_session, player_id: int, fps: int) -> dict:
    """Service implementation for B2 presigned upload flow."""
    video_record = await _create_upload_ticket(
        db_session=db_session,
        player_id=player_id,
        fps=fps,
        storage_backend="b2",
    )
    upload_url = generate_presigned_upload_url(
        object_key=video_record.video_key,
        # Do NOT sign Content-Type to avoid signature mismatch if the client
        # omits or varies the header during upload.
        content_type=None,
        expiry_seconds=600,
    )
    return {
        "upload_url": upload_url,
        "key": video_record.video_key,
        "upload_id": video_record.id,
    }


async def _issue_upload_ticket_local(db_session, player_id: int, fps: int) -> dict:
    """Service implementation for local test upload flow."""
    video_record = await _create_upload_ticket(
        db_session=db_session,
        player_id=player_id,
        fps=fps,
        storage_backend="local",
    )
    # Return an absolute URL so strict clients (e.g., Java URL parsers) accept it.
    # Prefer proxy-forwarded origin headers when present.
    forwarded_proto = (request.headers.get("X-Forwarded-Proto", "") or "").split(",")[0].strip()
    forwarded_host = (request.headers.get("X-Forwarded-Host", "") or "").split(",")[0].strip()
    if forwarded_host:
        scheme = forwarded_proto or request.scheme or "https"
        base_url = f"{scheme}://{forwarded_host}".rstrip("/")
    else:
        base_url = request.host_url.rstrip("/")
    return {
        "upload_url": f"{base_url}/video/local/test-upload/{video_record.id}",
        "upload_method": "PUT",
        "key": video_record.video_key,
        "upload_id": video_record.id,
    }


UPLOAD_TICKET_ISSUERS = {
    "b2": _issue_upload_ticket_b2,
    "local": _issue_upload_ticket_local,
}


@video_bp.get("/presigned_upload_url")
@rate_limit(limit=10, period=timedelta(seconds=1))
async def presigned_upload_url():
    """
    Generate a presigned PUT URL for direct-to-B2 video upload.

    Query Parameters:
        fps (int): Frame rate of the video (default: 20)
        acc_hash (str): Player's account hash for authentication

    Returns:
        JSON with upload_url and key on success
        HTTP 402 if missing premium upgrade or daily quota exceeded
        HTTP 400 if missing parameters
        HTTP 404 if player not found
        HTTP 500 on internal error
    """
    db_session = None
    try:
        fps = _normalize_fps(request.args.get("fps", "20"))
        acc_hash = request.args.get("acc_hash", "")

        # Look up the player
        db_session = get_db_session()
        player, _, daily_limit, today_count, error_response = await _resolve_upload_context(
            db_session, acc_hash
        )
        if error_response:
            return error_response

        active_backend = _get_active_video_backend()
        issue_impl = UPLOAD_TICKET_ISSUERS.get(active_backend, _issue_upload_ticket_b2)
        ticket = await issue_impl(db_session, player.player_id, fps)

        metrics.record_request("video_presigned_url", True, app="new_api")

        logger.log_sync(
            "info",
            (
                f"[Video] Presigned URL issued player_id={player.player_id} "
                f"fps={fps} backend={active_backend} key={ticket['key']}"
            ),
        )

        payload = {
            "upload_url": ticket["upload_url"],
            "upload_method": ticket.get("upload_method", "PUT"),
            "key": ticket["key"],
            "upload_id": ticket.get("upload_id"),
            "daily_limit": daily_limit,
            "daily_used": today_count + 1,
            "storage_backend": active_backend,
        }
        return jsonify(payload), 200

    except Exception as e:
        logger.log_sync("error", f"Error generating presigned upload URL: {e}")
        metrics.record_request("video_presigned_url", False, app="new_api")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
            reset_db_connections()


@video_bp.post("/video/local/test-upload-init")
@rate_limit(limit=10, period=timedelta(seconds=1))
async def local_test_upload_init():
    """
    Create a local-backend upload ticket for testing.

    This keeps the existing B2 routes unchanged while enabling direct upload
    to the same API machine for validation.
    """
    db_session = None
    try:
        data = await request.get_json(silent=True) or {}
        fps = _normalize_fps(data.get("fps", request.args.get("fps", "20")))
        acc_hash = data.get("acc_hash", request.args.get("acc_hash", ""))

        db_session = get_db_session()
        player, _, daily_limit, today_count, error_response = await _resolve_upload_context(
            db_session, acc_hash
        )
        if error_response:
            return error_response

        issue_impl = UPLOAD_TICKET_ISSUERS["local"]
        ticket = await issue_impl(db_session, player.player_id, fps)

        return jsonify(
            {
                "upload_url": ticket["upload_url"],
                "upload_method": ticket.get("upload_method", "PUT"),
                "key": ticket["key"],
                "upload_id": ticket["upload_id"],
                "daily_limit": daily_limit,
                "daily_used": today_count + 1,
            }
        ), 200
    except Exception as e:
        logger.log_sync("error", f"Error creating local test upload ticket: {e}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
            reset_db_connections()


@video_bp.put("/video/local/test-upload/<int:upload_id>")
@rate_limit(limit=10, period=timedelta(seconds=1))
async def local_test_upload_put(upload_id: int):
    """Receive a raw MJPEG payload directly and store it locally.

    Streams the request body to disk in chunks to avoid loading the full file
    into RAM. For PUT with raw binary body, acc_hash must be provided as a
    query parameter (?acc_hash=xxx) or X-Acc-Hash header.
    """
    db_session = None
    try:
        # For raw PUT, body is binary; acc_hash from query params or X-Acc-Hash header
        acc_hash = request.args.get("acc_hash") or request.headers.get("X-Acc-Hash", "")
        if not acc_hash:
            return jsonify({"error": "Missing acc_hash parameter"}), 400

        content_length = request.content_length
        if not content_length:
            return jsonify({"error": "Missing Content-Length header"}), 411
        if content_length > VIDEO_LOCAL_MAX_UPLOAD_BYTES:
            return jsonify(
                {
                    "error": "Upload payload too large",
                    "max_bytes": VIDEO_LOCAL_MAX_UPLOAD_BYTES,
                }
            ), 413

        db_session = get_db_session()
        player = await asyncio.to_thread(
            lambda: db_session.query(Player).filter(Player.account_hash == acc_hash).first()
        )
        if not player:
            return jsonify({"error": "Player not found"}), 404

        video_record = await asyncio.to_thread(
            lambda: db_session.query(VideoUpload).filter(VideoUpload.id == upload_id).first()
        )
        if not video_record:
            return jsonify({"error": "Upload ticket not found"}), 404
        if video_record.player_id != player.player_id:
            return jsonify({"error": "Upload ticket does not belong to this player"}), 403
        if (video_record.storage_backend or "b2") != "local":
            return jsonify({"error": "Upload ticket is not configured for local backend"}), 400
        if video_record.status != "pending":
            return jsonify({"error": f"Upload ticket is in '{video_record.status}' state"}), 400

        local_path = resolve_internal_path(video_record.video_key, backend="local")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Stream body to disk in chunks (avoids loading full file into RAM)
        bytes_written = await _stream_request_body_to_file(
            local_path,
            max_bytes=VIDEO_LOCAL_MAX_UPLOAD_BYTES,
        )
        if bytes_written is None:
            return jsonify(
                {
                    "error": "Upload payload too large",
                    "max_bytes": VIDEO_LOCAL_MAX_UPLOAD_BYTES,
                }
            ), 413
        if bytes_written == 0:
            return jsonify({"error": "Missing upload body"}), 400

        return jsonify(
            {
                "message": "Raw upload stored locally",
                "key": video_record.video_key,
                "bytes_written": bytes_written,
            }
        ), 200
    except Exception as e:
        logger.log_sync("error", f"Error uploading local test video payload: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
            reset_db_connections()


async def _stream_request_body_to_file(path: str, max_bytes: int) -> Optional[int]:
    """
    Stream the request body to disk in chunks. Returns bytes written, or None
    if the stream exceeded max_bytes. Uses minimal RAM regardless of file size.
    Writes are offloaded to a thread pool to avoid blocking the event loop.
    """
    total = 0
    try:
        with open(path, "wb") as f:
            async for chunk in request.body:
                total += len(chunk)
                if total > max_bytes:
                    return None
                await asyncio.to_thread(f.write, chunk)
        return total
    except Exception:
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass
        raise


async def _load_player_and_video_record(db_session, video_key: str, acc_hash: str):
    """Load and validate ownership context used by upload-complete handlers."""
    player = await asyncio.to_thread(
        lambda: db_session.query(Player).filter(Player.account_hash == acc_hash).first()
    )
    if not player:
        return None, None, (jsonify({"error": "Player not found"}), 404)

    video_record = await asyncio.to_thread(
        lambda: db_session.query(VideoUpload).filter(
            VideoUpload.video_key == video_key,
            VideoUpload.player_id == player.player_id,
        ).first()
    )
    if not video_record:
        return None, None, (jsonify({"error": "Video upload record not found"}), 404)

    if video_record.status != "pending":
        return None, None, (
            jsonify({"error": f"Video is in '{video_record.status}' state, expected 'pending'"}),
            400,
        )

    return player, video_record, None


async def _complete_upload_b2(db_session, video_key: str, acc_hash: str):
    """Legacy B2 completion behavior (no storage existence check)."""
    _, video_record, error_response = await _load_player_and_video_record(
        db_session, video_key, acc_hash
    )
    if error_response:
        return error_response

    video_record.status = "uploaded"
    await asyncio.to_thread(db_session.commit)
    return jsonify(
        {
            "message": "Upload confirmed, video queued for processing",
            "key": video_key,
            "status": "uploaded",
            "storage_backend": "b2",
        }
    ), 200


async def _complete_upload_internal(db_session, video_key: str, acc_hash: str):
    """
    Internal completion behavior:
    - Requires ticket/backend to be local
    - Verifies raw local object exists before queueing worker processing
    """
    _, video_record, error_response = await _load_player_and_video_record(
        db_session, video_key, acc_hash
    )
    if error_response:
        return error_response

    if (video_record.storage_backend or "b2") != "local":
        return jsonify({"error": "Video upload record is not local backend"}), 400

    exists = await object_exists(video_key, backend="local")
    if not exists:
        return jsonify({"error": "Raw local upload not found on disk"}), 404

    video_record.status = "uploaded"
    await asyncio.to_thread(db_session.commit)
    return jsonify(
        {
            "message": "Upload confirmed, video queued for processing",
            "key": video_key,
            "status": "uploaded",
            "storage_backend": "local",
        }
    ), 200


@video_bp.post("/video/local/test-upload-complete")
@rate_limit(limit=10, period=timedelta(seconds=1))
async def local_test_upload_complete():
    """
    Mark a local test upload as complete.

    Mirrors /video/upload-complete behavior, but validates local file presence.
    """
    db_session = None
    try:
        data = await request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        video_key = data.get("key", "")
        acc_hash = data.get("acc_hash", "")

        if not video_key:
            return jsonify({"error": "Missing 'key' field"}), 400
        if not acc_hash:
            return jsonify({"error": "Missing 'acc_hash' field"}), 400

        db_session = get_db_session()
        return await _complete_upload_internal(db_session, video_key, acc_hash)
    except Exception as e:
        logger.log_sync("error", f"Error confirming local test upload: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
            reset_db_connections()


@video_bp.post("/video/upload-complete")
@rate_limit(limit=10, period=timedelta(seconds=1))
async def video_upload_complete():
    """
    Notify the API that a video upload to B2 is complete.
    
    Called by the plugin after successfully uploading the raw MJPEG
    to the presigned URL. This transitions the record from "pending"
    to "uploaded" and enqueues it for background FFmpeg processing.

    JSON Body:
        key (str): The object key returned from /presigned_upload_url
        acc_hash (str): Player's account hash for authentication

    Returns:
        JSON with status confirmation
    """
    db_session = None
    try:
        data = await request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        video_key = data.get("key", "")
        acc_hash = data.get("acc_hash", "")

        if not video_key:
            return jsonify({"error": "Missing 'key' field"}), 400
        if not acc_hash:
            return jsonify({"error": "Missing 'acc_hash' field"}), 400

        db_session = get_db_session()
        active_backend = _get_active_video_backend()
        complete_impl = _complete_upload_internal if active_backend == "local" else _complete_upload_b2
        return await complete_impl(db_session, video_key, acc_hash)

    except Exception as e:
        logger.log_sync("error", f"Error confirming video upload: {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
            reset_db_connections()


@video_bp.get("/video/status")
@rate_limit(limit=20, period=timedelta(seconds=1))
async def video_status():
    """
    Get the processing status of a video upload.

    Query Parameters:
        key (str): The video object key

    Returns:
        JSON with current status and video URL if processed
    """
    db_session = None
    try:
        video_key = request.args.get("key", "")
        if not video_key:
            return jsonify({"error": "Missing 'key' parameter"}), 400

        db_session = get_db_session()
        video_record = await asyncio.to_thread(
            lambda: db_session.query(VideoUpload).filter(
                VideoUpload.video_key == video_key
            ).first()
        )

        if not video_record:
            return jsonify({"error": "Video not found"}), 404

        result = {
            "key": video_record.video_key,
            "status": video_record.status,
            "created_at": video_record.created_at.isoformat() if video_record.created_at else None,
        }

        if video_record.status == "processed" and video_record.final_key:
            # Generate the serving URL dynamically. This avoids persisting
            # expiring presigned URLs when B2_CDN_BASE_URL is not configured.
            result["video_url"] = get_public_video_url(
                video_record.final_key,
                backend=backend_for_video_record(video_record),
            )
        elif video_record.status == "failed":
            result["error_message"] = video_record.error_message

        return jsonify(result), 200

    except Exception as e:
        logger.log_sync("error", f"Error getting video status: {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
            reset_db_connections()

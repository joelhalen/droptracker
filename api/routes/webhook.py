import asyncio
import json as _json
import os
import uuid
from datetime import datetime, timedelta

from quart import Blueprint, jsonify, request, g
from quart_rate_limiter import rate_limit

from api.core import logger, get_db_session, metrics, reset_db_connections
from data import submissions
from db import Player, Drop
from db.models.video_upload import VideoUpload
from utils.download import download_image
from utils.video_storage import backend_for_video_record, get_public_video_url


webhook_bp = Blueprint("webhook", __name__)
MAIN_WORLD_TYPE = "main"


def _drop_request_debug(message: str):
    """Consistent logging for drop request ingestion diagnostics."""
    print(f"[DropRequestDebug] {message}")


def _normalize_world_type(raw_world_type):
    if raw_world_type is None:
        return MAIN_WORLD_TYPE
    normalized = str(raw_world_type).strip().lower()
    return normalized or MAIN_WORLD_TYPE


def _normalize_submission_type(raw_submission_type):
    normalized = str(raw_submission_type or "").strip().lower()
    match normalized:
        case "other" | "npc":
            return "drop"
        case "kill_time" | "npc_kill":
            return "personal_best"
        case "experience_update" | "experience_milestone" | "level_up":
            return "experience"
        case "quest_completion":
            return "quest"
        case _:
            return normalized


def _dispatch_non_main_submission(world_type, submission_type):
    """Route non-main submissions by type (currently no-op handlers)."""
    match world_type:
        case "seasonal":
            match submission_type:
                case (
                    "drop"
                    | "collection_log"
                    | "personal_best"
                    | "combat_achievement"
                    | "experience"
                    | "quest"
                    | "pet"
                    | "adventure_log"
                ):
                    # Seasonal routing placeholder for future handlers.
                    pass
                case _:
                    pass
        case _:
            # Ignore all unsupported world types for now.
            pass


async def _link_video_to_submission(processed_data, db_session):
    """
    If the processed webhook data contains a video_key, look up the
    corresponding VideoUpload record and link it to the submission.
    
    The video_key is injected into the webhook embed fields by the
    RuneLite plugin when video mode is active.
    """
    video_key = processed_data.get("video_key")
    if not video_key:
        return

    try:
        video_record = db_session.query(VideoUpload).filter(
            VideoUpload.video_key == video_key
        ).first()
        if video_record:
            # Link the submission type (canonicalized) + unique submission id (guid)
            sub_type_raw = (processed_data.get("type") or "").lower().strip()
            type_map = {
                "npc": "drop",
                "other": "drop",
                "drop": "drop",
                "collection_log": "collection_log",
                "personal_best": "personal_best",
                "kill_time": "personal_best",
                "npc_kill": "personal_best",
                "combat_achievement": "combat_achievement",
                "pet": "pet",
            }
            video_record.submission_type = type_map.get(sub_type_raw, sub_type_raw or None)
            video_record.submission_unique_id = processed_data.get("guid") or processed_data.get("unique_id")

            # If the video has already been processed, attach a URL (computed)
            if video_record.status == "processed" and video_record.final_key:
                processed_data["video_url"] = get_public_video_url(
                    video_record.final_key,
                    backend=backend_for_video_record(video_record),
                )
            elif video_record.status in ("pending", "uploaded"):
                # Mark as uploaded if still pending (upload confirmed by webhook arrival)
                if video_record.status == "pending":
                    video_record.status = "uploaded"
    except Exception as e:
        # Don't fail the submission if video linking fails
        print(f"[Webhook] Error linking video_key {video_key}: {e}")


async def _try_attach_video_url_to_drop(processed_data, db_session):
    """
    If this is a drop submission with a video_key, try to:
    - find the created Drop row via Drop.unique_id == guid
    - link VideoUpload.drop_id to that Drop
    - if the video is already processed, write drops.video_url
      (otherwise the client can poll /video/status by key)
    """
    try:
        video_key = processed_data.get("video_key")
        guid = processed_data.get("guid") or processed_data.get("unique_id")
        if not video_key or not guid:
            return

        # Find video upload record
        video_record = db_session.query(VideoUpload).filter(VideoUpload.video_key == video_key).first()
        if not video_record:
            return

        # Find the Drop created for this submission (drop_processor uses unique_id=guid)
        drop = db_session.query(Drop).filter(Drop.unique_id == str(guid)).order_by(Drop.drop_id.desc()).first()
        if not drop:
            return

        video_record.drop_id = drop.drop_id

        # If processed, persist the serving URL on the drop row
        if video_record.status == "processed" and video_record.final_key:
            drop.video_url = get_public_video_url(
                video_record.final_key,
                backend=backend_for_video_record(video_record),
            )
    except Exception as e:
        print(f"[Webhook] Error attaching video URL to drop: {e}")


@webhook_bp.post("/submit")
@rate_limit(limit=10, period=timedelta(seconds=1))
async def submit_data():
    return await webhook_data()


@webhook_bp.post("/webhook")
@rate_limit(limit=100, period=timedelta(seconds=1))
async def webhook_data():
    import time
    req_start = time.perf_counter()
    return await _process_webhook_request(req_start)


async def _process_webhook_request(req_start):
    import time

    success = False
    request_type = "webhook"
    submission_type = None
    db_session = None

    def log_phase(phase_name):
        elapsed = (time.perf_counter() - req_start) * 1000
        if elapsed > 500:
            print(f"[WebhookPhase] {phase_name}: {elapsed:.0f}ms")

    try:
        # Default request subtype for timing logs.
        g.submission_type = "webhook"
        content_type = request.headers.get('Content-Type', '')
        if 'multipart/form-data' in content_type:
            try:
                form = await request.form
                log_phase("form_parsed")
                
                payload_json = form.get('payload_json')
                if not payload_json:
                    return jsonify({"error": "No payload_json found in form data"}), 400

                import json
                webhook_payload = json.loads(payload_json)
                if webhook_payload is None:
                    return jsonify({"error": "Invalid JSON in payload_json"}), 400

                files = await request.files
                log_phase("files_read")
                
                image_file = files.get('file') if files else None

                processed_items = await process_webhook_data(webhook_payload)
                if not processed_items:
                    return jsonify({"error": "Could not process webhook data"}), 400

                submission_type = processed_items[0].get("type")
                g.submission_type = submission_type or "webhook"
                processed_items[0]["downloaded"] = False

                response = None
                db_session = get_db_session()
                log_phase("session_acquired")
                try:
                    downloaded = False
                    file_path = None
                    for processed_data in processed_items:
                        submission_type = processed_data.get("type")
                        g.submission_type = submission_type or g.submission_type
                        processed_data["downloaded"] = False
                        world_type = _normalize_world_type(processed_data.get("world_type"))
                        processed_data["world_type"] = world_type

                        if world_type != MAIN_WORLD_TYPE:
                            _dispatch_non_main_submission(
                                world_type,
                                _normalize_submission_type(submission_type),
                            )
                            continue

                        if image_file:
                            processed_data["has_image"] = True
                            player_name = processed_data.get('player', processed_data.get('player_name', None))
                            player = db_session.query(Player).filter(Player.player_name == player_name).first()
                            log_phase("player_lookup")
                            player_wom_id = player.wom_id if player else None
                            if player:
                                file_path = await download_image(
                                    sub_type=processed_data.get('type', 'unknown'),
                                    player=player,
                                    player_wom_id=player_wom_id,
                                    file_data=image_file,
                                    processed_data=processed_data
                                )
                                log_phase("image_downloaded")
                                if file_path:
                                    if processed_data.get("image_path"):
                                        file_path = processed_data["image_path"]
                                    processed_data["image_url"] = file_path
                                    processed_data["downloaded"] = True
                                    downloaded = True
                        else:
                            if file_path:
                                if processed_data.get("image_path"):
                                    processed_data["image_url"] = processed_data["image_path"]
                                else:
                                    processed_data["image_url"] = file_path
                        processed_data['used_api'] = True

                        try:
                            # Link video upload if video_key is present in the embed data
                            await _link_video_to_submission(processed_data, db_session)
                            match (submission_type):
                                case "drop" | "other"| "npc":
                                    submission_type = "drop"
                                    g.submission_type = submission_type
                                    _drop_request_debug(
                                        "dispatch_to_drop_processor "
                                        f"guid={processed_data.get('guid')} "
                                        f"player_name={processed_data.get('player_name') or processed_data.get('player')} "
                                        f"players_included_type={type(processed_data.get('players_included')).__name__} "
                                        f"players_included={processed_data.get('players_included')} "
                                        f"nearby_players_type={type(processed_data.get('nearby_players')).__name__} "
                                        f"nearby_players={processed_data.get('nearby_players')}"
                                    )
                                    response = await submissions.drop_processor(processed_data, external_session=db_session)
                                    log_phase("drop_processed")
                                    # After creating the drop, try to link the video to the Drop row
                                    await _try_attach_video_url_to_drop(processed_data, db_session)
                                case "collection_log":
                                    submission_type = "collection_log"
                                    g.submission_type = submission_type
                                    response = await submissions.clog_processor(processed_data, external_session=db_session)
                                    log_phase("clog_processed")
                                case "personal_best" | "kill_time" | "npc_kill":
                                    submission_type = "personal_best"
                                    g.submission_type = submission_type
                                    response = await submissions.pb_processor(processed_data, external_session=db_session)
                                    log_phase("pb_processed")
                                case "combat_achievement":
                                    submission_type = "combat_achievement"
                                    g.submission_type = submission_type
                                    response = await submissions.ca_processor(processed_data, external_session=db_session)
                                    log_phase("ca_processed")
                                case "experience_update" | "experience_milestone" | "level_up":
                                    g.submission_type = "experience"
                                    response = await submissions.experience_processor(processed_data, external_session=db_session)
                                    log_phase("experience_processed")
                                case "quest" | "quest_completion":
                                    submission_type = "quest"
                                    g.submission_type = submission_type
                                    response = await submissions.quest_processor(processed_data, external_session=db_session)
                                    log_phase("quest_processed")
                                case "pet":
                                    g.submission_type = "pet"
                                    response = await submissions.pet_processor(processed_data, external_session=db_session)
                                    log_phase("pet_processed")
                                case "adventure_log":
                                    g.submission_type = "adventure_log"
                                    print(f"Got adventure log data: {processed_data}")
                                    response = await submissions.adventure_log_processor(processed_data, external_session=db_session)
                                    log_phase("adventure_log_processed")
                                case _:
                                    g.submission_type = submission_type or "webhook"
                                    continue
                            db_session.commit()
                            log_phase("committed")
                        except Exception:
                            db_session.rollback()
                            raise

                    success = True
                except Exception as processor_error:
                    logger.log_sync("error", f"Processor error inside {submission_type} processor: {processor_error}")
                    return jsonify({"error": f"Error processing data: {str(processor_error)}"}), 200
                finally:
                    if db_session:
                        db_session.close()
                        db_session = None

                if response:
                    return jsonify({"message": response.message, "notice": response.notice}), 200
                else:
                    return jsonify({"message": "Webhook data processed successfully"}), 200

            except Exception as e:
                logger.log_sync("error", f"Error processing multipart request: {e}")
                return jsonify({"error": f"Error processing request: {str(e)}"}), 400
        else:
            try:
                data = await request.get_json()
                return jsonify({"message": "JSON data processed"}), 200
            except Exception as e:
                logger.log_sync("error", f"Error processing JSON request: {e}")
                return jsonify({"error": f"Error processing request: {str(e)}"}), 400
    except Exception as e:
        logger.log_sync("error", f"Webhook Exception: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
        reset_db_connections()
        if submission_type:
            metrics.record_request(submission_type, success, app="new_api")
        else:
            metrics.record_request(request_type, success, app="new_api")


async def process_webhook_data(webhook_data):
    try:
        embeds = webhook_data.get("embeds", [])
        if not embeds:
            print("No embeds found in webhook data")
            return None
        processed_items = []
        for embed in embeds:
            processed_data = {
                field["name"]: field["value"] for field in embed.get("fields", [])
            }
            if not processed_data.get("timestamp"):
                processed_data["timestamp"] = int(datetime.now().timestamp())
            processed_data["world_type"] = _normalize_world_type(processed_data.get("world_type"))
            submission_type = str(processed_data.get("type", "")).lower()
            if submission_type in ("drop", "npc", "other"):
                raw_nearby_players = processed_data.get("nearby_players")
                raw_players_included = processed_data.get("players_included")
                normalized_players = _parse_nearby_players(
                    raw_nearby_players if raw_nearby_players is not None else raw_players_included
                )
                processed_data["players_included"] = normalized_players
                processed_data["nearby_players"] = normalized_players
                _drop_request_debug(
                    "parsed_embed_fields "
                    f"guid={processed_data.get('guid')} "
                    f"player_name={processed_data.get('player_name') or processed_data.get('player')} "
                    f"item_name={processed_data.get('item_name') or processed_data.get('item')} "
                    f"source={processed_data.get('source') or processed_data.get('npc_name')} "
                    f"raw_players_included={raw_players_included} "
                    f"raw_nearby_players={raw_nearby_players} "
                    f"normalized_players_included={normalized_players}"
                )
            processed_items.append(processed_data)
        return processed_items
    except Exception as e:
        print(f"Error processing webhook data: {e}")
        return None


# ==============================================================================
# MANUAL SUBMISSION ENDPOINT
# ==============================================================================
# This endpoint is intended for manual submissions from the front-end web server.
# Authentication is handled by the PHP front-end before the request reaches here.
#
# SUPPORTS TWO REQUEST FORMATS:
#
# 1. JSON POST (Content-Type: application/json) - when no image is uploaded:
# {
#     "submission_type": string,  // Required. One of: "drop", "collection_log", 
#                                 //   "personal_best", "combat_achievement", "pet"
#     "player_name": string,      // Required. The player's OSRS username.
#     "image_url": string|null,   // Optional. URL to an already-uploaded image.
#     ... (type-specific fields below)
# }
#
# 2. MULTIPART FORM DATA - when an image file is uploaded:
#    - All fields sent as form fields
#    - Image sent as "image_file" field (CURLFile in PHP)
# "world_type": string|null,  // Optional. Defaults to "main" when omitted.
#
# === DROP SUBMISSION FIELDS ===
# submission_type: "drop"
# "item_name": string,        // Required. Name of the dropped item.
# "item_id": int|null,        // Optional. OSRS item ID if known.
# "npc_name": string,         // Required. Name of the NPC that dropped the item.
# "value": int,               // Required. GE value of the drop in GP.
# "quantity": int,            // Optional. Number of items dropped (default: 1).
# "kill_count": int|null,     // Optional. Kill count at time of drop.
# "nearby_players": list|string|null, // Optional. List of nearby player names for
#                             //   point splits. Accepts a JSON array, a
#                             //   comma-separated string, or a list.
#
# === COLLECTION LOG SUBMISSION FIELDS ===
# submission_type: "collection_log"
# "item_name": string,        // Required. Name of the collection log item.
# "kc": int|null,             // Optional. Kill count when item was obtained.
# "reported_slots": int|null, // Optional. Number of collection log slots filled.
#
# === PERSONAL BEST SUBMISSION FIELDS ===
# submission_type: "personal_best"
# "boss_name": string,        // Required. Name of the boss (alias: "npc_name").
# "time_ms": int,             // Required. Kill time in milliseconds.
# "is_pb": bool,              // Optional. Whether this is a new personal best (default: true).
# "team_size": int,           // Optional. Team size for raids (default: 1).
#
# === COMBAT ACHIEVEMENT SUBMISSION FIELDS ===
# submission_type: "combat_achievement"
# "task": string,             // Required. Name of the combat achievement task.
# "tier": string,             // Required. Tier: easy/medium/hard/elite/master/grandmaster.
# "points": int,              // Required. Points awarded for this task.
# "total_points": int,        // Required. Player's total CA points after completion.
# "completed": string|null,   // Optional. Tier milestone completed (e.g., "elite").
#
# === PET SUBMISSION FIELDS ===
# submission_type: "pet"
# "pet_name": string,         // Required. Name of the pet obtained.
# "source": string|null,      // Optional. Source NPC/activity name.
# "killcount": int|null,      // Optional. Kill count when pet was obtained.
# "duplicate": bool,          // Optional. Whether this is a duplicate pet (default: false).
# "milestone": string|null,   // Optional. Milestone info (e.g., "500 KC").
#
# RESPONSE FORMAT:
# Success: { "success": true, "message": string, "notice": string|null }
# Error:   { "success": false, "error": string }
#
# HTTP Status Codes:
# 200 - Success (also used for processor errors to allow error message display)
# 400 - Bad request (missing required fields, invalid submission_type)
# 500 - Internal server error
# ==============================================================================

def _parse_nearby_players(raw):
    """
    Accept nearby_players in multiple formats and return a list of player-name
    strings (or None when empty / absent).

    Supported inputs:
      - None / empty            -> None
      - list of strings         -> returned as-is (after stripping blanks)
      - JSON-encoded array str  -> parsed then returned
      - comma-separated string  -> split and stripped
    """
    if raw is None:
        return None
    if isinstance(raw, list):
        cleaned = [str(n).strip() for n in raw if n and str(n).strip()]
        return cleaned or None
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return None
        if raw.startswith("["):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list):
                    cleaned = [str(n).strip() for n in parsed if n and str(n).strip()]
                    return cleaned or None
            except (_json.JSONDecodeError, TypeError):
                pass
        cleaned = [p.strip() for p in raw.split(",") if p.strip()]
        return cleaned or None
    return None


@webhook_bp.post("/manual-submit")
@rate_limit(limit=10, period=timedelta(seconds=1))
async def manual_submit():
    """
    Handle manual submissions from the PHP front-end.
    
    This endpoint processes manually-submitted data for drops, collection log entries,
    personal bests, combat achievements, and pets. Unlike the webhook endpoint:
    - No account hash is required (authentication handled by front-end)
    - A unique submission ID is generated automatically
    - The submission is marked as using the API and considered pre-authenticated
    """
    import time
    req_start = time.perf_counter()
    return await _process_manual_submission(req_start)


async def _process_manual_submission(req_start):
    import time
    
    success = False
    submission_type = None
    db_session = None
    
    def log_phase(phase_name):
        elapsed = (time.perf_counter() - req_start) * 1000
        if elapsed > 100:
            print(f"[ManualSubmitPhase] {phase_name}: {elapsed:.0f}ms")
    
    try:
        # Determine content type and parse accordingly
        content_type = request.headers.get('Content-Type', '')
        image_file = None
        
        if 'multipart/form-data' in content_type:
            # Handle multipart form data (when image file is uploaded)
            try:
                form = await request.form
                files = await request.files
                log_phase("form_parsed")
                
                # Extract form fields into a dict, converting types as needed
                data = {}
                for key in form:
                    value = form[key]
                    # Convert string representations of booleans
                    if value.lower() == 'true':
                        data[key] = True
                    elif value.lower() == 'false':
                        data[key] = False
                    # Try to convert numeric strings
                    elif value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                        data[key] = int(value)
                    else:
                        data[key] = value if value else None
                
                # Get the uploaded image file
                image_file = files.get('image_file') if files else None
                log_phase("files_read")
                
            except Exception as e:
                logger.log_sync("error", f"Error parsing manual submission multipart form: {e}")
                return jsonify({"success": False, "error": f"Invalid form data: {str(e)}"}), 400
        else:
            # Handle JSON request body
            try:
                data = await request.get_json()
                if not data:
                    return jsonify({"success": False, "error": "No JSON data provided"}), 400
                log_phase("json_parsed")
            except Exception as e:
                logger.log_sync("error", f"Error parsing manual submission JSON: {e}")
                return jsonify({"success": False, "error": "Invalid JSON data"}), 400
        
        # Validate required fields
        submission_type = data.get("submission_type")
        player_name = data.get("player_name")
        
        if not submission_type:
            return jsonify({"success": False, "error": "Missing required field: submission_type"}), 400
        if not player_name:
            return jsonify({"success": False, "error": "Missing required field: player_name"}), 400
        
        # Normalize submission type
        submission_type = str(submission_type).lower().strip()
        valid_types = ["drop", "collection_log", "personal_best", "combat_achievement", "pet"]
        if submission_type not in valid_types:
            return jsonify({
                "success": False, 
                "error": f"Invalid submission_type: {submission_type}. Must be one of: {', '.join(valid_types)}"
            }), 400

        world_type = _normalize_world_type(data.get("world_type"))
        if world_type != MAIN_WORLD_TYPE:
            _dispatch_non_main_submission(world_type, _normalize_submission_type(submission_type))
            success = True
            return jsonify({
                "success": True,
                "message": f"Ignored {submission_type} submission for world_type '{world_type}'"
            }), 200
        
        # Generate a unique ID for this submission
        unique_id = str(uuid.uuid4())
        
        # Build processed data for the appropriate processor
        # Use a placeholder account hash that will pass the auth check
        placeholder_hash = f"manual_submission_{unique_id}"
        
        processed_data = {
            "player_name": player_name,
            "player": player_name,
            "acc_hash": placeholder_hash,
            "guid": unique_id,
            "used_api": True,
            "downloaded": False,
            "world_type": world_type,
            "image_url": data.get("image_url"),
            "timestamp": datetime.now().isoformat(),
        }
        
        # Acquire database session
        db_session = get_db_session()
        log_phase("session_acquired")
        
        # Look up the player to set the correct account hash for auth
        player = db_session.query(Player).filter(Player.player_name.ilike(player_name)).first()
        log_phase("player_lookup")
        
        if not player:
            return jsonify({
                "success": False, 
                "error": f"Player '{player_name}' not found in database. The player must have submitted at least one drop through the RuneLite plugin first."
            }), 400
        
        # Use the player's existing account hash so authentication passes
        processed_data["acc_hash"] = player.account_hash or placeholder_hash
        
        # Handle image file upload if present
        if image_file:
            processed_data["has_image"] = True
            player_wom_id = player.wom_id if player else None
            try:
                file_path = await download_image(
                    sub_type=submission_type,
                    player=player,
                    player_wom_id=player_wom_id,
                    file_data=image_file,
                    processed_data=processed_data
                )
                log_phase("image_downloaded")
                if file_path:
                    processed_data["image_url"] = file_path
                    processed_data["downloaded"] = True
            except Exception as img_error:
                logger.log_sync("error", f"Error downloading manual submission image: {img_error}")
                # Continue without image - don't fail the submission
        
        response = None
        
        try:
            match submission_type:
                case "drop":
                    # Validate drop-specific required fields
                    item_name = data.get("item_name")
                    npc_name = data.get("npc_name")
                    value = data.get("value")
                    quantity = data.get("quantity") or 1
                    
                    if not item_name:
                        return jsonify({"success": False, "error": "Drop submission requires 'item_name'"}), 400
                    if not npc_name:
                        return jsonify({"success": False, "error": "Drop submission requires 'npc_name'"}), 400
                    if value is None:
                        return jsonify({"success": False, "error": "Drop submission requires 'value'"}), 400
                    
                    nearby_players = _parse_nearby_players(
                        data.get("nearby_players") or data.get("players_included")
                    )
                    
                    processed_data.update({
                        "type": "drop",
                        "item_name": item_name,
                        "item": item_name,
                        "item_id": data.get("item_id"),
                        "npc_name": npc_name,
                        "value": int(value),
                        "quantity": int(quantity),
                        "kill_count": data.get("kill_count"),
                        "players_included": nearby_players,
                    })
                    response = await submissions.drop_processor(processed_data, external_session=db_session)
                    log_phase("drop_processed")
                
                case "collection_log":
                    item_name = data.get("item_name")
                    
                    if not item_name:
                        return jsonify({"success": False, "error": "Collection log submission requires 'item_name'"}), 400
                    
                    processed_data.update({
                        "type": "collection_log",
                        "item_name": item_name,
                        "item": item_name,
                        "kc": data.get("kc"),
                        "reported_slots": data.get("reported_slots"),
                    })
                    response = await submissions.clog_processor(processed_data, external_session=db_session)
                    log_phase("clog_processed")
                
                case "personal_best":
                    boss_name = data.get("boss_name") or data.get("npc_name")
                    time_ms = data.get("time_ms")
                    
                    if not boss_name:
                        return jsonify({"success": False, "error": "Personal best submission requires 'boss_name' or 'npc_name'"}), 400
                    if time_ms is None:
                        return jsonify({"success": False, "error": "Personal best submission requires 'time_ms'"}), 400
                    
                    # Handle is_pb - could be bool, string "true"/"false", or int 1/0
                    is_pb_raw = data.get("is_pb", True)
                    if isinstance(is_pb_raw, str):
                        is_pb = is_pb_raw.lower() in ('true', '1', 'yes')
                    elif isinstance(is_pb_raw, int):
                        is_pb = bool(is_pb_raw)
                    else:
                        is_pb = bool(is_pb_raw)
                    
                    processed_data.update({
                        "type": "personal_best",
                        "npc_name": boss_name,
                        "boss_name": boss_name,
                        "kill_time": int(time_ms),
                        "current_time_ms": int(time_ms),
                        "personal_best_ms": int(time_ms),
                        "is_pb": is_pb,
                        "is_new_pb": is_pb,
                        "team_size": int(data.get("team_size") or 1),
                    })
                    response = await submissions.pb_processor(processed_data, external_session=db_session)
                    log_phase("pb_processed")
                
                case "combat_achievement":
                    task = data.get("task")
                    tier = data.get("tier")
                    points = data.get("points")
                    total_points = data.get("total_points")
                    
                    if not task:
                        return jsonify({"success": False, "error": "Combat achievement submission requires 'task'"}), 400
                    if not tier:
                        return jsonify({"success": False, "error": "Combat achievement submission requires 'tier'"}), 400
                    if points is None:
                        return jsonify({"success": False, "error": "Combat achievement submission requires 'points'"}), 400
                    if total_points is None:
                        return jsonify({"success": False, "error": "Combat achievement submission requires 'total_points'"}), 400
                    
                    processed_data.update({
                        "type": "combat_achievement",
                        "player_name": player_name,
                        "task": task,
                        "tier": str(tier).lower(),
                        "points": int(points),
                        "total_points": int(total_points),
                        "completed": data.get("completed"),
                    })
                    response = await submissions.ca_processor(processed_data, external_session=db_session)
                    log_phase("ca_processed")
                
                case "pet":
                    pet_name = data.get("pet_name")
                    
                    if not pet_name:
                        return jsonify({"success": False, "error": "Pet submission requires 'pet_name'"}), 400
                    
                    # Handle duplicate boolean
                    duplicate_raw = data.get("duplicate", False)
                    if isinstance(duplicate_raw, str):
                        duplicate = duplicate_raw.lower() in ('true', '1', 'yes')
                    elif isinstance(duplicate_raw, int):
                        duplicate = bool(duplicate_raw)
                    else:
                        duplicate = bool(duplicate_raw)
                    
                    processed_data.update({
                        "type": "pet",
                        "pet_name": pet_name,
                        "source": data.get("source"),
                        "killcount": data.get("killcount"),
                        "duplicate": duplicate,
                        "milestone": data.get("milestone"),
                        "previously_owned": data.get("previously_owned"),
                        "game_message": data.get("game_message"),
                    })
                    response = await submissions.pet_processor(processed_data, external_session=db_session)
                    log_phase("pet_processed")
            
            db_session.commit()
            log_phase("committed")
            success = True
            
            if response:
                return jsonify({
                    "success": True,
                    "message": response.message,
                    "notice": response.notice
                }), 200
            else:
                return jsonify({
                    "success": True,
                    "message": f"Manual {submission_type} submission processed successfully"
                }), 200
        
        except Exception as processor_error:
            logger.log_sync("error", f"Manual submission processor error: {processor_error}")
            if db_session:
                db_session.rollback()
            return jsonify({
                "success": False,
                "error": f"Error processing submission: {str(processor_error)}"
            }), 200
    
    except Exception as e:
        logger.log_sync("error", f"Manual submission exception: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    
    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
        reset_db_connections()
        if submission_type:
            metrics.record_request(f"manual_{submission_type}", success, app="new_api")
        else:
            metrics.record_request("manual_submit", success, app="new_api")

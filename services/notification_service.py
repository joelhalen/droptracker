import asyncio
import json
import os
from urllib.parse import quote
import shutil
from datetime import datetime, timedelta
import interactions
import aiohttp
import aiofiles
from sqlalchemy import text
from db.models import (
    ItemList,
    NotificationQueue,
    NpcList,
    PersonalBestEntry,
    User,
    UserConfiguration,
    get_current_partition,
    session,
    Player,
    Group,
    GroupConfiguration,
)
from db.ops import DatabaseOperations, associate_player_ids, get_formatted_name
from db.xf.upgrades import check_active_upgrade
from utils.redis import redis_client
from utils.embeds import update_boss_pb_embed
from utils.messages import confirm_new_npc, confirm_new_item, name_change_message, new_player_message
from utils.format import format_number, replace_placeholders, convert_from_ms
from utils.download import download_player_image
from db.app_logger import AppLogger
from utils import osrs_api
from services.xf_services import get_user_id, create_alert
from utils.wiseoldman import fetch_group_members
from services.redis_updates import get_player_list_loot_sum, loot_tracker, get_player_current_month_total
from db.models.video_upload import VideoUpload
from utils.video_storage import (
    VIDEO_LOCAL_DELETE_AFTER_NOTIFY,
    backend_for_video_record,
    delete_object,
    get_public_video_url,
)

app_logger = AppLogger()
global_footer = os.getenv('DISCORD_MESSAGE_FOOTER')
db = DatabaseOperations()


# Removed global tracking dictionaries - now using database-based tracking via NotifiedSubmission table

class NotificationService:
    def __init__(self, bot: interactions.Client, db_ops: DatabaseOperations):
        self.bot = bot
        self.db_ops = db_ops
        self.notified_users = []
        self.running = False
        self._processing_lock = asyncio.Lock()
        # Background task that runs the processing loop
        self._task = None
        # Configurable sleep interval (in seconds)
        self.sleep_interval = 3
        # Statistics tracking
        self.processed_count = 0
        self.last_processed_at = None
        self._video_notif_dir = os.getenv("VIDEO_NOTIFICATION_DIR", "/tmp/droptracker-video-notifs")
        self._video_notif_ttl_seconds = int(os.getenv("VIDEO_NOTIFICATION_TTL_SECONDS", "1800"))  # 30 min
        self._video_notif_max_bytes = int(os.getenv("VIDEO_NOTIFICATION_MAX_BYTES", str(5 * 1024 * 1024)))  # 5MB

    @staticmethod
    def _coerce_int(value):
        """Best-effort integer parsing for mixed payload values."""
        if value in (None, ""):
            return None
        try:
            return int(str(value).replace(",", "").strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_group_points_members(value) -> str:
        """Render awarded member details into a concise placeholder-friendly string."""
        if not isinstance(value, list) or not value:
            return ""
        parts = []
        for member in value:
            if not isinstance(member, dict):
                continue
            name = str(member.get("player_name") or member.get("player_id") or "Unknown")
            awarded = member.get("points_awarded", 0)
            total = member.get("current_points", 0)
            parts.append(f"{name} (+{awarded}, total {total})")
        return ", ".join(parts)

    @staticmethod
    def _is_same_member_as_target(member: dict, target_player_name: str, target_player_id) -> bool:
        """Return True when an awarded member matches the notification target player."""
        if not isinstance(member, dict):
            return False

        member_id = member.get("player_id")
        target_id = target_player_id
        if member_id not in (None, "") and target_id not in (None, ""):
            try:
                return int(str(member_id).strip()) == int(str(target_id).strip())
            except (TypeError, ValueError):
                pass

        member_name = str(member.get("player_name") or "").strip().lower()
        target_name = str(target_player_name or "").strip().lower()
        return bool(member_name and target_name and member_name == target_name)

    @staticmethod
    def _strip_group_points_placeholders(text: str) -> str:
        if text is None:
            return text
        cleaned = str(text)
        for placeholder in (
            "{group_points_awarded}",
            "{group_points_receiver_total}",
            "{group_points_member_count}",
            "{group_points_members_awarded}",
        ):
            cleaned = cleaned.replace(placeholder, "")
        while "  " in cleaned:
            cleaned = cleaned.replace("  ", " ")
        return cleaned.strip()

    def _finalize_group_points_embed(self, embed: interactions.Embed) -> interactions.Embed:
        """Remove unresolved group-point placeholders and empty fields safely."""
        if not embed:
            return embed

        if getattr(embed, "title", None) and "{group_points_" in embed.title:
            embed.title = self._strip_group_points_placeholders(embed.title) or None
        if getattr(embed, "description", None) and "{group_points_" in embed.description:
            embed.description = self._strip_group_points_placeholders(embed.description) or None
        if getattr(embed, "footer", None) and getattr(embed.footer, "text", None):
            if "{group_points_" in embed.footer.text:
                embed.footer.text = self._strip_group_points_placeholders(embed.footer.text) or ""

        if embed.fields:
            kept_fields = []
            for field in embed.fields:
                field_name = getattr(field, "name", "") or ""
                field_value = getattr(field, "value", "") or ""
                combined = f"{field_name} {field_value}"
                if "{group_points_" in combined:
                    # Placeholder not replaced -> data was not provided, omit this field.
                    continue
                if str(field_value).strip() == "":
                    continue
                kept_fields.append(field)
            embed.fields = kept_fields
        return embed

    def _group_points_placeholder_map(self, data: dict) -> dict:
        members = data.get("group_points_members_awarded") or []
        members_text = self._format_group_points_members(members)
        points_awarded = self._coerce_int(data.get("group_points_awarded")) or 0
        receiver_total = self._coerce_int(data.get("group_points_receiver_total")) or 0
        member_count = self._coerce_int(data.get("group_points_member_count"))
        if member_count is None:
            member_count = len(members) if isinstance(members, list) else 0

        target_player_name = data.get("player_name")
        target_player_id = data.get("player_id")
        suppress_members_awarded = (
            isinstance(members, list)
            and len(members) == 1
            and self._is_same_member_as_target(members[0], target_player_name, target_player_id)
        )

        values = {}
        # Only show award-related placeholders when points were actually awarded.
        if points_awarded > 0:
            values["{group_points_awarded}"] = str(points_awarded)
        if member_count > 0 and members_text and not suppress_members_awarded:
            values["{group_points_member_count}"] = str(member_count)
            values["{group_points_members_awarded}"] = members_text
        # Current total can be shown even if this event awarded zero points.
        if receiver_total > 0:
            values["{group_points_receiver_total}"] = str(receiver_total)
        return values

    def _build_default_quest_embed(self, data: dict, player_name: str, player_id: int, video_url: str = "") -> interactions.Embed:
        """
        Build a default quest embed when no DB-backed template exists.
        """
        quest_name = str(data.get("quest_name") or "Unknown quest")
        quests_completed = self._coerce_int(data.get("quests_completed"))
        total_quests = self._coerce_int(data.get("total_quests"))
        completion_percentage = str(data.get("completion_percentage") or "N/A")
        quest_points = self._coerce_int(data.get("quest_points"))
        total_quest_points = self._coerce_int(data.get("total_quest_points"))
        qp_percentage = str(data.get("qp_percentage") or "N/A")
        submitted_at = self._coerce_int(data.get("timestamp"))

        player_link = f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)"
        embed = interactions.Embed(
            title="Quest Completed",
            description=f"{player_link} completed **{quest_name}**.",
            color="#5A8DEE",
        )
        embed.add_field(
            name="Quest Progress",
            value=f"`{quests_completed if quests_completed is not None else '?'}`/`{total_quests if total_quests is not None else '?'}` ({completion_percentage})",
            inline=True,
        )
        embed.add_field(
            name="Quest Points",
            value=f"`{quest_points if quest_points is not None else '?'}`/`{total_quest_points if total_quest_points is not None else '?'}` ({qp_percentage})",
            inline=True,
        )
        if video_url:
            embed.add_field(name="Video", value=f"[Watch clip]({video_url})", inline=False)
        embed.set_footer(global_footer)
        return embed

    def _maybe_get_video_url(self, db_session, data: dict) -> str:
        """
        Best-effort resolution of a public video URL for notifications.

        Order of precedence:
        1) explicit video_url in notification data
        2) lookup via video_key in VideoUpload (only if processed)
        """
        try:
            direct = data.get("video_url")
            if direct:
                return str(direct)
            video_key = data.get("video_key")
            if not video_key:
                return ""
            rec = (
                db_session.query(VideoUpload)
                .filter(VideoUpload.video_key == video_key)
                .first()
            )
            if rec and rec.status == "processed" and rec.final_key:
                return get_public_video_url(
                    rec.final_key,
                    backend=backend_for_video_record(rec),
                )
        except Exception:
            pass
        return ""

    async def _cleanup_processed_local_video_after_send(self, db_session, data: dict) -> None:
        """
        Best-effort cleanup of processed local videos after a successful notification send.
        """
        if not VIDEO_LOCAL_DELETE_AFTER_NOTIFY:
            return
        try:
            video_key = data.get("video_key")
            if not video_key:
                return
            rec = (
                db_session.query(VideoUpload)
                .filter(VideoUpload.video_key == video_key)
                .first()
            )
            if not rec or rec.status != "processed" or not rec.final_key:
                return
            if backend_for_video_record(rec) != "local":
                return
            await delete_object(rec.final_key, backend="local")
        except Exception:
            # Best effort only
            return

    async def _cleanup_old_video_attachments(self) -> None:
        """Delete old temp MP4 files for notification attachments."""
        try:
            os.makedirs(self._video_notif_dir, exist_ok=True)
            now_ts = datetime.now().timestamp()
            for name in os.listdir(self._video_notif_dir):
                if not name.endswith(".mp4"):
                    continue
                path = os.path.join(self._video_notif_dir, name)
                try:
                    st = os.stat(path)
                    if now_ts - st.st_mtime > self._video_notif_ttl_seconds:
                        os.remove(path)
                except FileNotFoundError:
                    continue
                except Exception:
                    continue
        except Exception:
            # Best effort cleanup only
            return

    async def _download_video_attachment(self, video_url: str, notification_id: int) -> tuple[interactions.File | None, str | None]:
        """
        Download an MP4 to a temp directory and return an interactions.File attachment.

        Returns:
            (attachment, local_path)
        """
        if not video_url:
            return None, None

        await self._cleanup_old_video_attachments()
        os.makedirs(self._video_notif_dir, exist_ok=True)

        # Create a deterministic filename per notification to avoid collisions
        file_name = f"notif_{notification_id}.mp4"
        local_path = os.path.join(self._video_notif_dir, file_name)

        try:
            # Local-path mode (used by local storage backend)
            source_path = video_url
            if source_path.startswith("file://"):
                source_path = source_path[7:]
            if os.path.exists(source_path):
                size = os.path.getsize(source_path)
                if size <= 0 or size > self._video_notif_max_bytes:
                    return None, None
                await asyncio.to_thread(shutil.copyfile, source_path, local_path)
                return interactions.File(local_path), local_path

            # Remote URL mode (existing B2/CDN behavior)
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session_http:
                async with session_http.get(video_url) as resp:
                    if resp.status not in (200, 206):
                        return None, None

                    # Enforce a max size to avoid abuse/unexpected large downloads
                    content_length = resp.headers.get("Content-Length")
                    if content_length:
                        try:
                            if int(content_length) > self._video_notif_max_bytes:
                                return None, None
                        except Exception:
                            pass

                    size = 0
                    async with aiofiles.open(local_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(1024 * 256):
                            if not chunk:
                                continue
                            size += len(chunk)
                            if size > self._video_notif_max_bytes:
                                return None, None
                            await f.write(chunk)

            if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
                return None, None

            return interactions.File(local_path), local_path
        except Exception:
            return None, None

    def _should_defer_for_video(self, db_session, data: dict) -> tuple[bool, str]:
        """
        Decide whether we should defer sending this notification because it has an
        associated video that is not finished processing yet.

        Rules:
        - If no video_key is present: do not defer
        - If VideoUpload not found: do not defer (fallback to sending without video)
        - If VideoUpload status is pending/uploaded/processing: defer
        - If VideoUpload status is processed: do not defer
        - If VideoUpload status is failed: do not defer (avoid blocking forever)
        """
        try:
            video_key = data.get("video_key")
            if not video_key:
                return False, ""

            rec = (
                db_session.query(VideoUpload)
                .filter(VideoUpload.video_key == video_key)
                .first()
            )
            if not rec:
                return False, ""

            status = (rec.status or "").lower().strip()
            if status in ("pending", "uploaded", "processing"):
                return True, f"Waiting for video processing (status={status})"
            if status in ("processed", "failed"):
                return False, ""
            return False, ""
        except Exception:
            return False, ""
    
    #@interactions.Task.create(interactions.IntervalTrigger(seconds=5))
    async def start(self):
        """Start the notification service.

        Creates a long-lived background task bound to the current running loop
        that periodically processes pending notifications. If the task already
        exists and is running, this is a no-op.
        """
        # If the task is already running, do nothing
        if self._task is not None and not self._task.done():
            self.running = True
            app_logger.log(log_type="info", data="Notification service already running", app_name="notification_service", description="start")
            return

        # If a previous task finished, drop the reference
        self._task = None
        self.running = True

        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self.process_notifications_loop(), name="notification_service_loop")
        app_logger.log(log_type="info", data="Notification service started successfully", app_name="notification_service", description="start")
    
    async def stop(self):
        """Stop the notification service.

        Cancels and awaits the background task to ensure a clean shutdown.
        """
        self.running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

    def is_running(self) -> bool:
        """Return True if the background loop task is alive and running."""
        return self.running and self._task is not None and not self._task.done()
    
    def get_statistics(self) -> dict:
        """Get service statistics for monitoring"""
        return {
            "running": self.is_running(),
            "processed_count": self.processed_count,
            "last_processed_at": self.last_processed_at.isoformat() if self.last_processed_at else None,
            "sleep_interval": self.sleep_interval
        }
    
    async def get_pending_notifications_count(self) -> dict:
        """Get count of pending notifications by type for debugging"""
        try:
            from api.core import get_db_session
            with get_db_session() as db_session:
                pending_count = db_session.query(NotificationQueue).filter(
                    NotificationQueue.status == 'pending'
                ).count()
                
                processing_count = db_session.query(NotificationQueue).filter(
                    NotificationQueue.status == 'processing'
                ).count()
                
                failed_count = db_session.query(NotificationQueue).filter(
                    NotificationQueue.status == 'failed'
                ).count()
                
                # Get breakdown by notification type
                type_breakdown = {}
                for notification_type in ['drop', 'pb', 'ca', 'clog', 'pet', 'new_npc', 'new_item', 'name_change', 'new_player', 'user_upgrade', 'group_upgrade', 'update_log', 'points_earned']:
                    count = db_session.query(NotificationQueue).filter(
                        NotificationQueue.status == 'pending',
                        NotificationQueue.notification_type == notification_type
                    ).count()
                    if count > 0:
                        type_breakdown[notification_type] = count
                
                return {
                    "pending": pending_count,
                    "processing": processing_count,
                    "failed": failed_count,
                    "type_breakdown": type_breakdown
                }
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error getting pending notifications count: {e}", app_name="notification_service", description="get_pending_notifications_count")
            return {"error": str(e)}
    
    def set_sleep_interval(self, interval: int):
        """Set the sleep interval between processing cycles (in seconds)"""
        if interval < 1:
            raise ValueError("Sleep interval must be at least 1 second")
        self.sleep_interval = interval
        app_logger.log(log_type="info", data=f"Notification service sleep interval set to {interval}s", app_name="notification_service", description="set_sleep_interval")
    
    async def force_process_notifications(self):
        """Force immediate processing of pending notifications without affecting the main loop.
        
        This method can be called externally to trigger immediate notification processing
        without interfering with the existing background loop. It's safe to call multiple
        times concurrently as it uses the same locking mechanism as the main loop.
        
        Returns:
            int: Number of notifications processed in this call
        """
        if not self.is_running():
            app_logger.log(log_type="warning", data="Force processing called but service is not running", app_name="notification_service", description="force_process_notifications")
            return 0
        
        try:
            async with self._processing_lock:
                processed_count = await self.process_pending_notifications()
                
                # Update statistics
                if processed_count > 0:
                    self.processed_count += processed_count
                    self.last_processed_at = datetime.now()
                    app_logger.log(log_type="info", data=f"Force processing completed: {processed_count} notifications processed", app_name="notification_service", description="force_process_notifications")
                
                return processed_count
                
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error in force processing: {e}", app_name="notification_service", description="force_process_notifications")
            return 0
    
    
    async def process_notifications_loop(self):
        """Main loop to process notifications.

        This loop is resilient to transient errors and will continue running
        until explicitly stopped via `stop()`. It handles task cancellation
        gracefully to integrate with application shutdown.
        """
        cleanup_counter = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        app_logger.log(log_type="info", data=f"Notification service loop started with {self.sleep_interval}s interval", app_name="notification_service", description="process_notifications_loop")
        
        while self.running:
            try:
                async with self._processing_lock:
                    #app_logger.log(log_type="debug", data="Notification service processing cycle started", app_name="notification_service", description="process_notifications_loop")
                    processed_count = await self.process_pending_notifications()
                    
                    # Update statistics
                    if processed_count > 0:
                        self.processed_count += processed_count
                        self.last_processed_at = datetime.now()
                        #app_logger.log(log_type="info", data=f"Processed {processed_count} notifications in this cycle", app_name="notification_service", description="process_notifications_loop")
                    # else:
                    #     app_logger.log(log_type="debug", data="No notifications to process in this cycle", app_name="notification_service", description="process_notifications_loop")
                    
                    # Reset error counter on successful processing
                    consecutive_errors = 0
                    
                    # Clean up tracking dictionaries and stuck notifications every 1000 iterations
                    # Only run cleanup if we've actually processed some notifications
                    cleanup_counter += 1
                    if cleanup_counter >= 1000 and self.processed_count > 0:
                        app_logger.log(log_type="info", data="Starting cleanup cycle", app_name="notification_service", description="process_notifications_loop")
                        await self.cleanup_tracking_dicts()
                        await self.cleanup_stuck_notifications()
                        cleanup_counter = 0
                        app_logger.log(log_type="info", data=f"Cleanup completed. Total processed: {self.processed_count}", app_name="notification_service", description="process_notifications_loop")
            except asyncio.CancelledError:
                # Graceful shutdown
                app_logger.log(log_type="info", data="Notification service loop cancelled", app_name="notification_service", description="process_notifications_loop")
                break
            except Exception as e:
                consecutive_errors += 1
                app_logger.log(log_type="error", data=f"Error processing notifications (attempt {consecutive_errors}): {e}", app_name="notification_service", description="process_notifications_loop")
                
                # If we have too many consecutive errors, increase sleep time
                if consecutive_errors >= max_consecutive_errors:
                    app_logger.log(log_type="warning", data=f"Too many consecutive errors ({consecutive_errors}), increasing sleep time", app_name="notification_service", description="process_notifications_loop")
                    await asyncio.sleep(10)  # Longer sleep on repeated errors
                    consecutive_errors = 0  # Reset after longer sleep
                    continue  # Skip the normal sleep interval after error recovery
            
            # Normal sleep time between iterations - ensures regular processing
            await asyncio.sleep(self.sleep_interval)
    
    async def process_pending_notifications(self):
        """Process pending notifications with improved locking strategy"""
        processed_count = 0
        try:
            # Use a fresh session for this operation
            from api.core import get_db_session
            with get_db_session() as db_session:
                # First, get a small batch of pending notifications without locking
                # Include deferred notifications waiting on video, but only retry them
                # at a modest interval to avoid tight loops.
                retry_cutoff = datetime.now() - timedelta(seconds=15)
                notifications = (
                    db_session.query(NotificationQueue)
                    .filter(
                        (NotificationQueue.status == "pending")
                        | (
                            (NotificationQueue.status == "video_processing")
                            & (
                                (NotificationQueue.processed_at.is_(None))
                                | (NotificationQueue.processed_at < retry_cutoff)
                            )
                        )
                    )
                    .order_by(NotificationQueue.created_at.asc())
                    .limit(10)
                    .all()
                )  # Increased batch size for better efficiency
                
                og_length = len(notifications)
                if og_length == 0:
                    # No pending notifications found, return immediately
                    return processed_count
                    
                app_logger.log(log_type="info", data=f"Processing {og_length} pending notifications", app_name="notification_service", description="process_pending_notifications")
                
                for notification in notifications:
                    try:
                        # Use a more targeted locking approach - lock only the specific row
                        locked_notification = db_session.query(NotificationQueue).filter(
                            NotificationQueue.id == notification.id,
                            NotificationQueue.status.in_(["pending", "video_processing"])
                        ).with_for_update(skip_locked=True).first()
                        
                        # Skip if already locked by another process or no longer pending
                        if not locked_notification:
                            continue
                            
                        # Mark as processing immediately after acquiring lock
                        locked_notification.status = 'processing'
                        db_session.commit()
                        
                        # Process the notification with the same session
                        await self.process_notification_with_session(locked_notification, db_session)
                        processed_count += 1
                        
                    except Exception as e:
                        # Ensure we have a valid notification object for error handling
                        if 'locked_notification' in locals() and locked_notification:
                            locked_notification.status = 'failed'
                            locked_notification.error_message = str(e)
                            db_session.commit()
                        app_logger.log(log_type="error", data=f"Error processing notification {notification.id}: {e}", app_name="notification_service", description="process_pending_notifications")
            
            return processed_count
            
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error in process_pending_notifications: {e}", app_name="notification_service", description="process_pending_notifications")
            return processed_count

    async def process_notification_with_session(self, notification: NotificationQueue, db_session):
        """Process a single notification based on its type with a specific session"""
        try:
            app_logger.log(log_type="info", data=f"Processing notification {notification.id} of type '{notification.notification_type}'", app_name="notification_service", description="process_notification")
            
            data = json.loads(notification.data)
            notification_type = notification.notification_type

            # If this notification includes a video_key, delay sending until the
            # video processing pipeline has finished (so embeds can include the URL).
            should_defer, defer_reason = self._should_defer_for_video(db_session, data)
            if should_defer:
                notification.status = "video_processing"
                notification.error_message = defer_reason
                # Reuse processed_at as "last checked at" while deferred; once sent,
                # processed_at will be overwritten with the actual sent time.
                notification.processed_at = datetime.now()
                db_session.commit()
                return

            # If a video is available, expose it as video_url.
            # Do NOT overwrite image_url because many senders treat image_url as
            # a local-file-backed screenshot for attachments.
            video_url = self._maybe_get_video_url(db_session, data)
            if video_url:
                data["video_url"] = video_url
            
            # Check for duplicates before processing
            if not await self._is_not_sent_with_session(notification, data, db_session):
                app_logger.log(log_type="info", data=f"Notification {notification.id} was already sent, skipping", app_name="notification_service", description="process_notification")
                # Mark as sent since it was already processed
                notification.status = 'sent'
                notification.processed_at = datetime.now()
                db_session.commit()
                return
            
            # Process the notification based on its type
            # Pass the db_session to each method for consistent session handling
            if notification_type == 'drop':
                await self.send_drop_notification_with_session(notification, data, db_session)
            elif notification_type == 'pb':
                await self.send_pb_notification_with_session(notification, data, db_session)
            elif notification_type == 'ca':
                await self.send_ca_notification_with_session(notification, data, db_session)
            elif notification_type == 'clog':
                await self.send_clog_notification_with_session(notification, data, db_session)
            elif notification_type == 'pet':
                await self.send_pet_notification_with_session(notification, data, db_session)
            elif notification_type == 'level_up':
                await self.send_level_up_notification_with_session(notification, data, db_session)
            elif notification_type == 'quest':
                await self.send_quest_notification_with_session(notification, data, db_session)
            elif notification_type == 'new_npc':
                await self.send_new_npc_notification_with_session(notification, data, db_session)
            elif notification_type == 'new_item':
                await self.send_new_item_notification_with_session(notification, data, db_session)
            elif notification_type == 'name_change':
                await self.send_name_change_notification_with_session(notification, data, db_session)
            elif notification_type == 'new_player':
                await self.send_new_player_notification_with_session(notification, data, db_session)
            elif notification_type == 'user_upgrade':
                await self.send_user_upgrade_notification_with_session(notification, data, db_session)
            elif notification_type == 'group_upgrade':
                await self.send_group_upgrade_notification_with_session(notification, data, db_session)
            elif notification_type == 'update_log':
                await self.send_update_log_data_with_session(notification, data, db_session)
            elif notification_type == 'points_earned':
                await self.send_points_notification_with_session(notification, data, db_session)
            else:
                notification.status = 'failed'
                notification.error_message = f"Unknown notification type: {notification_type}"
                print(f"Notification type not found: '{notification_type}'")
                db_session.commit()
        except interactions.errors.Forbidden:
            group_id = data.get('group_id', None)
            if group_id:
                group = db_session.query(Group).filter(Group.group_id == group_id).first()
                if group:
                    guild_id = group.guild_id
                    authorized_users = await get_authorized_users(group_id)
                    for user in authorized_users:
                        try:
                            discord_user = await self.bot.fetch_user(user_id=user)
                            await discord_user.send(f"Hey, <@{discord_user}>!\n" + 
                            f"We just tried to process a `{notification_type}` submission for your group, however the <@{self.bot.user.id}> does not have permissions to send messages in the channel you configured...\n\n" + 
                            f"Please make sure the bot has **Manage Messages** and **Read Message History** permissions in the channel(s) you are using for notifications!")
                        except Exception as e:
                            print("Couldn't DM server admin about failed notification (bot permissions in server)...")
            print(f"Forbidden access error attempting to send a notification to {group_id}")
            # Mark as failed and commit
            notification.status = 'failed'
            notification.error_message = "Forbidden access error"
            db_session.commit()
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error processing notification {notification.id}: {e}", app_name="notification_service", description="process_notification")
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def process_notification(self, notification: NotificationQueue):
        """Process a single notification based on its type (legacy method for backward compatibility)"""
        from api.core import get_db_session
        with get_db_session() as db_session:
            await self.process_notification_with_session(notification, db_session)

    async def send_level_up_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a level up notification to Discord with session"""
        notification.status = 'processing'
        db_session.commit()
        try:
            group_id = notification.group_id
            player_id = notification.player_id

            # Channel: reuse loot channel unless you add a dedicated config key later
            channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_levels'
            ).first()

            if not channel_id_config or not channel_id_config.config_value:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                db_session.commit()
                return

            channel = await self.bot.fetch_channel(channel_id=channel_id_config.config_value)
            if channel is None:
                notification.status = 'failed'
                notification.error_message = f"Channel not found for group {group_id}"
                db_session.commit()
                return

            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('level_up', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('level_up', 1)

            if not embed_template:
                notification.status = 'failed'
                notification.error_message = f"No embed template for group {group_id}"
                db_session.commit()
                return

            # Data
            player_name = data.get("player_name") or ""
            image_url = data.get("image_url") or ""
            skills_text = data.get("skills_text") or ""
            if not skills_text:
                # Backwards compat with older payloads (single skill)
                sn = data.get("skill_name") or ""
                nl = data.get("new_level")
                lg = data.get("levels_gained")
                if sn and nl is not None:
                    if lg is not None:
                        skills_text = f"{sn} {nl} (+{lg})"
                    else:
                        skills_text = f"{sn} {nl}"

            formatted_name = get_formatted_name(player_name, group_id, db_session)

            replacements = {
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                "{skill_name}": str(data.get("skill_name") or data.get("skills_names") or ""),
                "{skills_names}": str(data.get("skills_names") or ""),
                "{skills_text}": str(skills_text or ""),
                "{new_level}": str(data.get("new_level") or ""),
                "{levels_gained}": str(data.get("levels_gained") or ""),
                "{xp_total}": str(data.get("xp_total") or ""),
                "{total_level}": str(data.get("total_level") or ""),
                "{total_xp}": str(data.get("total_xp") or ""),
                "{combat_level}": str(data.get("combat_level") or ""),
                "{image_url}": image_url,
                "{video_url}": "",
                "{video_link}": "",
            }

            embed = replace_placeholders(embed_template, replacements)
            if group_id == 2:
                embed = await self.remove_group_field(embed)

            content = f"{formatted_name} levelled-up:"

            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        await channel.send(content, embed=embed, files=attachment)
                    else:
                        await channel.send(content, embed=embed)
                except Exception:
                    await channel.send(content, embed=embed)
            else:
                await channel.send(content, embed=embed)

            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_level_up_notification(self, notification: NotificationQueue, data: dict):
        """Send a level up notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            group_id = notification.group_id
            player_id = notification.player_id

            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
            ).first()
            if not channel_id_config or not channel_id_config.config_value:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return

            channel = await self.bot.fetch_channel(channel_id=channel_id_config.config_value)
            if channel is None:
                notification.status = 'failed'
                notification.error_message = f"Channel not found for group {group_id}"
                session.commit()
                return

            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('level_up', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('level_up', 1)
            if not embed_template:
                notification.status = 'failed'
                notification.error_message = f"No embed template for group {group_id}"
                session.commit()
                return

            player_name = data.get("player_name") or ""
            image_url = data.get("image_url") or ""
            skills_text = data.get("skills_text") or ""
            if not skills_text:
                sn = data.get("skill_name") or ""
                nl = data.get("new_level")
                lg = data.get("levels_gained")
                if sn and nl is not None:
                    if lg is not None:
                        skills_text = f"{sn} {nl} (+{lg})"
                    else:
                        skills_text = f"{sn} {nl}"

            formatted_name = get_formatted_name(player_name, group_id, session)
            raw_xp = str(data.get("total_xp") or "")
            total_xp = format_number(raw_xp)
            raw_xp_total = str(data.get("xp_total") or "")
            xp_total = format_number(raw_xp_total)
            replacements = {
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                "{skill_name}": str(data.get("skill_name") or data.get("skills_names") or ""),
                "{skills_names}": str(data.get("skills_names") or ""),
                "{skills_text}": str(skills_text or ""),
                "{new_level}": str(data.get("new_level") or ""),
                "{levels_gained}": str(data.get("levels_gained") or ""),
                "{xp_total}": total_xp,
                "{total_level}": str(data.get("total_level") or ""),
                "{total_xp}": xp_total,
                "{combat_level}": str(data.get("combat_level") or ""),
                "{image_url}": image_url,
                "{video_url}": "",
                "{video_link}": "",
            }

            embed = replace_placeholders(embed_template, replacements)
            if group_id == 2:
                embed = await self.remove_group_field(embed)

            content = f"{formatted_name} leveled up!"

            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        await channel.send(content, embed=embed, files=attachment)
                    else:
                        await channel.send(content, embed=embed)
                except Exception:
                    await channel.send(content, embed=embed)
            else:
                await channel.send(content, embed=embed)

            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise

    async def send_quest_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a quest completion notification to Discord with session"""
        notification.status = 'processing'
        db_session.commit()
        try:
            group_id = notification.group_id
            player_id = notification.player_id

            # Channel config (inferred). Fallback to loot channel.
            channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_quests'
            ).first()
            if not channel_id_config or not channel_id_config.config_value:
                channel_id_config = db_session.query(GroupConfiguration).filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
            if not channel_id_config or not channel_id_config.config_value:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                db_session.commit()
                return

            channel = await self.bot.fetch_channel(channel_id=channel_id_config.config_value)
            if channel is None:
                notification.status = 'failed'
                notification.error_message = f"Channel not found for group {group_id}"
                db_session.commit()
                return

            # Embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('quest', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('quest', 1)

            player_name = data.get("player_name") or ""
            quest_name = data.get("quest_name") or ""
            image_url = data.get("image_url") or ""

            video_url = self._maybe_get_video_url(db_session, data)

            formatted_name = get_formatted_name(player_name, group_id, db_session)
            if embed_template:
                replacements = {
                    "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                    "{quest_name}": str(quest_name),
                    "{quests_completed}": str(data.get("quests_completed") or ""),
                    "{total_quests}": str(data.get("total_quests") or ""),
                    "{completion_percentage}": str(data.get("completion_percentage") or ""),
                    "{quest_points}": str(data.get("quest_points") or ""),
                    "{total_quest_points}": str(data.get("total_quest_points") or ""),
                    "{qp_percentage}": str(data.get("qp_percentage") or ""),
                    "{timestamp}": str(data.get("timestamp") or ""),
                    "{video_url}": video_url or "",
                    "{video_link}": f"[Video]({video_url})" if video_url else "",
                    # Prefer video for display; keep screenshot in data["image_url"] for attachments
                    "{image_url}": video_url or image_url or "",
                }

                embed = replace_placeholders(embed_template, replacements)
                if group_id == 2:
                    embed = await self.remove_group_field(embed)
            else:
                embed = self._build_default_quest_embed(
                    data=data,
                    player_name=player_name,
                    player_id=player_id,
                    video_url=video_url,
                )

            content = f"{formatted_name} completed a quest!"

            # Prefer attaching MP4 if available; otherwise attach screenshot if present.
            video_attachment, video_local_path = (None, None)
            if video_url:
                video_attachment, video_local_path = await self._download_video_attachment(video_url, notification.id)

            try:
                if video_attachment:
                    await channel.send(content, embed=embed, files=video_attachment)
                elif image_url:
                    try:
                        local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                        if os.path.exists(local_path):
                            attachment = interactions.File(local_path)
                            await channel.send(content, embed=embed, files=attachment)
                        else:
                            await channel.send(content, embed=embed)
                    except Exception:
                        await channel.send(content, embed=embed)
                else:
                    await channel.send(content, embed=embed)
            finally:
                if video_local_path:
                    try:
                        os.remove(video_local_path)
                    except Exception:
                        pass

            await self._cleanup_processed_local_video_after_send(db_session, data)
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()

        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_quest_notification(self, notification: NotificationQueue, data: dict):
        """Send a quest completion notification to Discord (legacy)"""
        notification.status = 'processing'
        session.commit()
        try:
            group_id = notification.group_id
            player_id = notification.player_id

            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_quests'
            ).first()
            if not channel_id_config or not channel_id_config.config_value:
                channel_id_config = session.query(GroupConfiguration).filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
            if not channel_id_config or not channel_id_config.config_value:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return

            channel = await self.bot.fetch_channel(channel_id=channel_id_config.config_value)
            if channel is None:
                notification.status = 'failed'
                notification.error_message = f"Channel not found for group {group_id}"
                session.commit()
                return

            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('quest', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('quest', 1)

            player_name = data.get("player_name") or ""
            quest_name = data.get("quest_name") or ""
            image_url = data.get("image_url") or ""
            video_url = self._maybe_get_video_url(session, data)

            formatted_name = get_formatted_name(player_name, group_id, session)
            if embed_template:
                replacements = {
                    "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                    "{quest_name}": str(quest_name),
                    "{quests_completed}": str(data.get("quests_completed") or ""),
                    "{total_quests}": str(data.get("total_quests") or ""),
                    "{completion_percentage}": str(data.get("completion_percentage") or ""),
                    "{quest_points}": str(data.get("quest_points") or ""),
                    "{total_quest_points}": str(data.get("total_quest_points") or ""),
                    "{qp_percentage}": str(data.get("qp_percentage") or ""),
                    "{timestamp}": str(data.get("timestamp") or ""),
                    "{video_url}": video_url or "",
                    "{video_link}": f"[Video]({video_url})" if video_url else "",
                    "{image_url}": video_url or image_url or "",
                }

                embed = replace_placeholders(embed_template, replacements)
                if group_id == 2:
                    embed = await self.remove_group_field(embed)
            else:
                embed = self._build_default_quest_embed(
                    data=data,
                    player_name=player_name,
                    player_id=player_id,
                    video_url=video_url,
                )

            content = f"{formatted_name} completed a quest!"

            video_attachment, video_local_path = (None, None)
            if video_url:
                video_attachment, video_local_path = await self._download_video_attachment(video_url, notification.id)

            try:
                if video_attachment:
                    await channel.send(content, embed=embed, files=video_attachment)
                elif image_url:
                    try:
                        local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                        if os.path.exists(local_path):
                            attachment = interactions.File(local_path)
                            await channel.send(content, embed=embed, files=attachment)
                        else:
                            await channel.send(content, embed=embed)
                    except Exception:
                        await channel.send(content, embed=embed)
                else:
                    await channel.send(content, embed=embed)
            finally:
                if video_local_path:
                    try:
                        os.remove(video_local_path)
                    except Exception:
                        pass

            await self._cleanup_processed_local_video_after_send(session, data)
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()

        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise

    async def send_update_log_data_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a update log data notification to Discord with session"""
        notification.status = 'processing'
        db_session.commit()
        try:
            channel_id = 1210765287591256084
            channel = await self.bot.fetch_channel(channel_id=channel_id)
            if channel:
                updates = data.get('updates')
                updates = ["- " + update + "\n" for update in updates]
                text = f"### A new update log has been published:\n\n"
                text += f"".join(updates)
                await channel.send(text)
                notification.status = 'sent'
                notification.processed_at = datetime.now()
                db_session.commit()
            else:
                notification.status = 'failed'
                notification.error_message = f"Channel not found"
                db_session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            app_logger.log(log_type="error", data=f"Error sending update log data notification: {e}", app_name="notification_service", description="send_update_log_data")
            raise

    async def send_update_log_data(self, notification: NotificationQueue, data: dict):
        """Send a update log data notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            channel_id = 1210765287591256084
            channel = await self.bot.fetch_channel(channel_id=channel_id)
            if channel:
                updates = data.get('updates')
                updates = ["- " + update + "\n" for update in updates]
                text = f"### A new update log has been published:\n\n"
                text += f"".join(updates)
                await channel.send(text)
                notification.processed_at = datetime.now()
                session.commit()
            else:
                notification.status = 'failed'
                notification.error_message = f"Channel not found"
                session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            app_logger.log(log_type="error", data=f"Error sending update log data notification: {e}", app_name="notification_service", description="send_update_log_data")
            raise
    
    async def send_points_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a points earned notification to Discord with session"""
        try:
            scope_earned = data.get('scope_earned')
            if not scope_earned:
                return
            match scope_earned:
                case 'player':
                    user_id = data.get('user_id')
                    user = db_session.query(User).filter(User.user_id == user_id).first()
                case 'group':
                    group_id = data.get('group_id')
                    group = db_session.query(Group).filter(Group.group_id == group_id).first()
            if not user and not group:
                return
            if user:
                user_name = user.username
            else:
                user_name = group.group_name
            amount_earned = data.get('amount_earned')
            source = data.get('source')
            new_total = data.get('current_total')
            comment = data.get('comment')
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            app_logger.log(log_type="error", data=f"Error sending points earned notification: {e}", app_name="notification_service", description="send_points_notification")
            raise

    async def send_points_notification(self, notification: NotificationQueue, data: dict):
        """Send a points earned notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            scope_earned = data.get('scope_earned')
            if not scope_earned:
                return
            match scope_earned:
                case 'player':
                    user_id = data.get('user_id')
                    user = session.query(User).filter(User.user_id == user_id).first()
                case 'group':
                    group_id = data.get('group_id')
                    group = session.query(Group).filter(Group.group_id == group_id).first()
            if not user and not group:
                return
            if user:
                user_name = user.username
            else:
                user_name = group.group_name
            amount_earned = data.get('amount_earned')
            source = data.get('source')
            new_total = data.get('current_total')
            comment = data.get('comment')
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            app_logger.log(log_type="error", data=f"Error sending points earned notification: {e}", app_name="notification_service", description="send_points_notification")
            raise

    async def send_group_upgrade_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a group upgrade notification to Discord with session"""
        try:
            group_id = data.get('group_id')
            group = db_session.query(Group).filter(Group.group_id == group_id).first()
            user_id = data.get('dt_id')
            user = db_session.query(User).filter(User.user_id == user_id).first()
            status = data.get('status') 
            if user.players:
                players = [player for player in user.players]
                player_name = players[0].player_name
            else:
                player_name = None
            global_embed = None
            group_embed = None
            global_channel = None
            channel = None
            if group and user:
                bot: interactions.Client = self.bot
                guild_id = group.guild_id
                guild = await bot.fetch_guild(guild_id)
                if guild:
                    channel = guild.public_updates_channel
                global_channel = await bot.fetch_channel(1373331322709479485)
                if channel:
                    match status:
                        case 'added':
                            group_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> Your group has been upgraded!",
                                description=f"<@{user.discord_id}> has upgraded {group.group_name} to unlock premium features, such as customizable embeds!",
                                color="#00f0f0"
                            )
                            group_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            group_embed.add_field(
                                name="Thank you for your support!",
                                value="Developing and maintaining a project like this takes lots of time and effort. We're extremely grateful for your continued support!"
                            )
                            group_embed.set_footer(global_footer)
                            global_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> `{user.username}` just upgraded {group.group_name}!",
                                description=f"{player_name if player_name else f'<@{user.discord_id}>'} just used their [account upgrade benefits](https://www.droptracker.io/groups/upgrades) to unlock premium features for [{group.group_name}](https://www.droptracker.io/groups/{group.group_name}.{group.group_id}/view)",
                                color="#00f0f0"
                            )
                            global_embed.add_field(
                                name="Thank you for your support!",
                                value="Contributions like this keep us motivated to continue maintaining the project."
                            )
                            global_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            global_embed.set_footer(global_footer)
                            global_guild = await bot.fetch_guild(1172737525069135962)
                            guild_member = await global_guild.fetch_member(user.discord_id)
                            if guild_member:
                                premium_role = global_guild.get_role(role_id=1210765189625151592)
                                await guild_member.add_role(role=premium_role)
                        case 'expired':
                            group_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> Your group has been downgraded!",
                                description=f"Your group upgrade has now expired.",
                                color="#f00000"
                            )
                            group_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            group_embed.add_field(
                                name="Thank you for your support!",
                                value="Developing and maintaining a project like this takes lots of time and effort. We're extremely grateful for any support you provided."
                            )
                            group_embed.set_footer(global_footer)
                            global_guild = await bot.fetch_guild(1172737525069135962)
                            guild_member = await global_guild.fetch_member(user.discord_id)
                            if guild_member:
                                premium_role = global_guild.get_role(role_id=1210765189625151592)
                                if premium_role in guild_member.roles:
                                    await guild_member.remove_role(role=premium_role)
                    if channel and group_embed:
                        try:
                            await channel.send(embed=group_embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                        except Exception as e:
                            if group.configurations:
                                for config in group.configurations:
                                    if config.config_key == 'authed_users':
                                        authed_users = config.config_value
                                        authed_users = authed_users.replace('[','').replace(']','').replace('"','').replace(' ', '').split(',')
                                        for user_id in authed_users:
                                            user_id = int(user_id)
                                            try:
                                                authed_user = await bot.fetch_user(user_id)
                                                if authed_user:
                                                    if status == 'expired':
                                                        group_embed.add_field(
                                                        name=f"Original Supporter:",
                                                        value=f"<@{user.discord_id}>",
                                                        inline=False
                                                    )
                                                    if user_id in self.notified_users:
                                                        ## Don't notify the same user twice in quick succession.
                                                        return
                                                    self.notified_users.append(user_id)
                                                    await authed_user.send(embed=group_embed)
                                                    await asyncio.sleep(0.2)
                                            except Exception as e:
                                                app_logger.log(log_type="error", data=f"Error sending group embed to authed user {user_id}: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                            app_logger.log(log_type="error", data=f"Error sending group embed: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                    else:
                        app_logger.log(log_type="error", data=f"Channel or group embed not found", app_name="notification_service", description="send_group_upgrade_notification")
                    if global_channel and global_embed:
                        try:
                            await global_channel.send(embed=global_embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            db_session.commit()
                        except Exception as e:
                            app_logger.log(log_type="error", data=f"Error sending global embed: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                    else:
                        app_logger.log(log_type="error", data=f"Global channel or global embed not found", app_name="notification_service", description="send_group_upgrade_notification")
                    
                    db_session.commit()
                else:
                    notification.status = 'failed'
                    notification.error_message = f"Channel not found"
                    db_session.commit()
            else:
                notification.status = 'failed'
                notification.error_message = f"Group not found"
                db_session.commit()
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_group_upgrade_notification(self, notification: NotificationQueue, data: dict):
        """Send a group upgrade notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            group_id = data.get('group_id')
            group = session.query(Group).filter(Group.group_id == group_id).first()
            user_id = data.get('dt_id')
            user = session.query(User).filter(User.user_id == user_id).first()
            status = data.get('status') 
            if user.players:
                players = [player for player in user.players]
                player_name = players[0].player_name
            else:
                player_name = None
            global_embed = None
            group_embed = None
            global_channel = None
            channel = None
            if group and user:
                bot: interactions.Client = self.bot
                guild_id = group.guild_id
                guild = await bot.fetch_guild(guild_id)
                if guild:
                    channel = guild.public_updates_channel
                global_channel = await bot.fetch_channel(1373331322709479485)
                if channel:
                    match status:
                        case 'added':
                            group_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> Your group has been upgraded!",
                                description=f"<@{user.discord_id}> has upgraded {group.group_name} to unlock premium features, such as customizable embeds!",
                                color="#00f0f0"
                            )
                            group_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            group_embed.add_field(
                                name="Thank you for your support!",
                                value="Developing and maintaining a project like this takes lots of time and effort. We're extremely grateful for your continued support!"
                            )
                            group_embed.set_footer(global_footer)
                            global_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> `{user.username}` just upgraded {group.group_name}!",
                                description=f"{player_name if player_name else f'<@{user.discord_id}>'} just used their [account upgrade benefits](https://www.droptracker.io/groups/upgrades) to unlock premium features for [{group.group_name}](https://www.droptracker.io/groups/{group.group_name}.{group.group_id}/view)",
                                color="#00f0f0"
                            )
                            global_embed.add_field(
                                name="Thank you for your support!",
                                value="Contributions like this keep us motivated to continue maintaining the project."
                            )
                            global_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            global_embed.set_footer(global_footer)
                            global_guild = await bot.fetch_guild(1172737525069135962)
                            guild_member = await global_guild.fetch_member(user.discord_id)
                            if guild_member:
                                premium_role = global_guild.get_role(role_id=1210765189625151592)
                                await guild_member.add_role(role=premium_role)
                        case 'expired':
                            group_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> Your group has been downgraded!",
                                description=f"Your group upgrade has now expired.",
                                color="#f00000"
                            )
                            group_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            group_embed.add_field(
                                name="Thank you for your support!",
                                value="Developing and maintaining a project like this takes lots of time and effort. We're extremely grateful for any support you provided."
                            )
                            group_embed.set_footer(global_footer)
                            global_guild = await bot.fetch_guild(1172737525069135962)
                            guild_member = await global_guild.fetch_member(user.discord_id)
                            if guild_member:
                                premium_role = global_guild.get_role(role_id=1210765189625151592)
                                if premium_role in guild_member.roles:
                                    await guild_member.remove_role(role=premium_role)
                    if channel and group_embed:
                        try:
                            await channel.send(embed=group_embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                        except Exception as e:
                            if group.configurations:
                                for config in group.configurations:
                                    if config.config_key == 'authed_users':
                                        authed_users = config.config_value
                                        authed_users = authed_users.replace('[','').replace(']','').replace('"','').replace(' ', '').split(',')
                                        for user_id in authed_users:
                                            user_id = int(user_id)
                                            try:
                                                authed_user = await bot.fetch_user(user_id)
                                                if authed_user:
                                                    if status == 'expired':
                                                        group_embed.add_field(
                                                        name=f"Original Supporter:",
                                                        value=f"<@{user.discord_id}>",
                                                        inline=False
                                                    )
                                                    if user_id in self.notified_users:
                                                        ## Don't notify the same user twice in quick succession.
                                                        return
                                                    self.notified_users.append(user_id)
                                                    await authed_user.send(embed=group_embed)
                                                    await asyncio.sleep(0.2)
                                            except Exception as e:
                                                app_logger.log(log_type="error", data=f"Error sending group embed to authed user {user_id}: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                            app_logger.log(log_type="error", data=f"Error sending group embed: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                    else:
                        app_logger.log(log_type="error", data=f"Channel or group embed not found", app_name="notification_service", description="send_group_upgrade_notification")
                    if global_channel and global_embed:
                        try:
                            await global_channel.send(embed=global_embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            session.commit()
                        except Exception as e:
                            app_logger.log(log_type="error", data=f"Error sending global embed: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                    else:
                        app_logger.log(log_type="error", data=f"Global channel or global embed not found", app_name="notification_service", description="send_group_upgrade_notification")
                    
                    session.commit()
                else:
                    notification.status = 'failed'
                    notification.error_message = f"Channel not found"
                    session.commit()
            else:
                notification.status = 'failed'
                notification.error_message = f"Group not found"
                session.commit()
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise

    async def send_user_upgrade_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a user upgrade notification to Discord with session"""
        try:
            user_id = data.get('dt_id')
            status = data.get('status')
            db_user = db_session.query(User).filter(User.user_id == user_id).first()
            if user_id in self.notified_users:
                ## Don't notify the same user twice in quick succession.
                return
            self.notified_users.append(user_id)
            if db_user:
                bot: interactions.Client = self.bot
                user = await bot.fetch_user(db_user.discord_id)
                if user:
                    match status:
                        case 'added':
                            embed = interactions.Embed(
                                title="<a:droptracker:1346787143778963497> Thank you for your support!",
                                description=f"Your account upgrade has been successfully processed.",
                                color="#00f0f0"
                            )
                            embed.add_field(
                                name="What's next?",
                                value="You can now [select a group](https://www.droptracker.io/account/premium)" + 
                                " to use your premium features on.\n\n" + 
                                "If you have any questions, [feel free to reach out in our Discord](https://www.droptracker.io/discord)"
                            )
                            embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            embed.set_footer(global_footer)
                            await user.send(embed=embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            db_session.commit()
                            return
                        case 'expired':
                            embed = interactions.Embed(
                                title="We're sorry to see you go!",
                                description=f"Your account upgrade has expired.\n" +
                                "Please consider [re-upgrading your account](https://www.droptracker.io/groups/upgrades) to continue supporting the project," + 
                                " and to retain access to your group's premium features.",
                                color="#f00000"
                            )
                            embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            
                            embed.set_footer(global_footer)
                            await user.send(embed=embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            db_session.commit()
                            return
            else:
                notification.status = 'failed'
                notification.error_message = f"User not found"
                db_session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            app_logger.log(log_type="error", data=f"Error sending user upgrade notification: {e}", app_name="notification_service", description="send_user_upgrade_notification")
            raise

    async def send_user_upgrade_notification(self, notification: NotificationQueue, data: dict):
        """Send a user upgrade notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            user_id = data.get('dt_id')
            status = data.get('status')
            db_user = session.query(User).filter(User.user_id == user_id).first()
            if user_id in self.notified_users:
                ## Don't notify the same user twice in quick succession.
                return
            self.notified_users.append(user_id)
            if db_user:
                bot: interactions.Client = self.bot
                user = await bot.fetch_user(db_user.discord_id)
                if user:
                    match status:
                        case 'added':
                            embed = interactions.Embed(
                                title="<a:droptracker:1346787143778963497> Thank you for your support!",
                                description=f"Your account upgrade has been successfully processed.",
                                color="#00f0f0"
                            )
                            embed.add_field(
                                name="What's next?",
                                value="You can now [select a group](https://www.droptracker.io/account/premium)" + 
                                " to use your premium features on.\n\n" + 
                                "If you have any questions, [feel free to reach out in our Discord](https://www.droptracker.io/discord)"
                            )
                            embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            embed.set_footer(global_footer)
                            await user.send(embed=embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            session.commit()
                            return
                        case 'expired':
                            embed = interactions.Embed(
                                title="We're sorry to see you go!",
                                description=f"Your account upgrade has expired.\n" +
                                "Please consider [re-upgrading your account](https://www.droptracker.io/groups/upgrades) to continue supporting the project," + 
                                " and to retain access to your group's premium features.",
                                color="#f00000"
                            )
                            embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            
                            embed.set_footer(global_footer)
                            await user.send(embed=embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            session.commit()
                            return
            else:
                notification.status = 'failed'
                notification.error_message = f"User not found"
                session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            app_logger.log(log_type="error", data=f"Error sending user upgrade notification: {e}", app_name="notification_service", description="send_user_upgrade_notification")
            raise
                
    async def send_drop_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a drop notification to Discord with session"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print(f"Got raw drop notification data: {data}")
            drop_id = data.get('drop_id')


            
            # Get channel ID for this group
            channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
            ).first()

            existing_notification = db_session.query(NotifiedSubmission).filter(
                NotifiedSubmission.player_id == player_id,
                NotifiedSubmission.group_id == group_id,
                NotifiedSubmission.drop_id == drop_id
            ).first()
            if existing_notification:
                print(f"Drop was already notified... Skipping")
                return
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                db_session.commit()
                return
            
            channel_id = channel_id_config.config_value
            channel = None
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                db_session.commit()
                return
            if channel is None:
                notification.status = 'failed'
                notification.error_message = f"Channel not found for group {group_id}"
                db_session.commit()
                return
            
            # Get player name
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            kill_count = data.get('kill_count', None)
            item_id = db_session.query(ItemList).filter(ItemList.item_name == item_name).first()
            if item_id:
                item_id = item_id.item_id
            else:
                item_id = 1
            npc_name = data.get('npc_name', None)
            if npc_name:
                npc_id = db_session.query(NpcList).filter(NpcList.npc_name == npc_name).first()
            else:
                npc_id = 0
            if npc_id:
                npc_id = npc_id.npc_id
            else:
                npc_id = 1
            value = data.get('value')
            quantity = data.get('quantity')
            total_value = data.get('total_value')
            image_url = data.get('image_url', None)
            if image_url is None or image_url == "":
                try:
                    drop = db_session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
                    if drop:
                        image_url = drop.image_url
                except Exception as e:
                    image_url = None
            #print(f"Debug - image_url: {image_url}, type: {type(image_url)}")
            if not image_url or "droptracker.io" not in image_url:
                image_url = ""

            # Best-effort video URL (may be empty if still processing)
            video_url = ""
            try:
                drop = db_session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
                if drop and getattr(drop, "video_url", None):
                    video_url = drop.video_url
            except Exception:
                pass
            if not video_url:
                video_url = self._maybe_get_video_url(db_session, data)
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('drop', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('drop', 1)
            #print(f"Debug - embed_template: {embed_template}")
            
            if not embed_template:
                notification.status = 'failed'
                notification.error_message = f"No embed template for group {group_id}"
                db_session.commit()
                return
            
            # Download image if available
            attachment = None
            if image_url:
                try:
                    # Convert external URL to local path matching the actual storage structure
                    local_url = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_url):
                        attachment = interactions.File(local_url)
                    else:
                        print(f"Debug - Image file not found at: {local_url}")
                        attachment = None
                except Exception as e:
                    print(f"Debug - Couldn't get attachment from path: {e}")
                    attachment = None
                    pass
            
            # Replace placeholders in embed
            player = None
            if not player_id:
                player = db_session.query(Player).filter(Player.player_name == player_name).first()
                if player:
                    player_id = player.player_id
            
            partition = get_current_partition()
            # Use monthly total computed by redis_updates player cache
            month_total_int = self._get_player_month_total(player_id, partition)
            player_month_total = format_number(month_total_int)
            players_in_group = db_session.query(Player.player_id).join(Player.groups).filter(Group.group_id == group_id).all()
            group_month_total = format_number(get_player_list_loot_sum([player.player_id for player in players_in_group]))
            # Use centralized rank helper for accuracy
            global_rank_data = loot_tracker.get_player_rank(player_id, None, partition)
            group_rank_data = loot_tracker.get_player_rank(player_id, group_id, partition)
            #print(f"Got group rank data: {group_rank_data}")
            print(f"Got global rank data: {global_rank_data}")
            if group_rank_data:
                group_rank, user_count = group_rank_data
            else:
                group_rank, user_count = None, redis_client.client.zcard(f"leaderboard:{partition}:group:{group_id}")
            if global_rank_data:
                global_rank, total_global_players = global_rank_data
            else:
                global_rank, total_global_players = None, redis_client.client.zcard(f"leaderboard:{partition}")
            # get all group ranks
            all_groups = db_session.query(Group.group_id).filter(Group.group_id != 2).all()
            #all_groups = db_session.query(Group.group_id).all()
            total_groups = len(all_groups) - 1
            group_totals = []
            for group in all_groups:
                group_total = redis_client.zsum(f"leaderboard:{partition}:group:{group.group_id}")
                group_totals.append({'id': group.group_id,
                                   'total': group_total})
            sorted_groups = sorted(group_totals, key=lambda x: x['total'], reverse=True)
            group_to_group_rank = str(next((i for i, g in enumerate(sorted_groups) if g['id'] == group_id), 0) + 1)
            formatted_name = get_formatted_name(player_name, group_id, db_session)
            content = f"{formatted_name} received a drop:"
            # Build rank strings safely
            if global_rank is not None and total_global_players is not None:
                global_rank_str = "`" + str(global_rank) + "`" + "/" + "`" + str(total_global_players) + "`"
            else:
                global_rank_str = "`?`"
            if group_rank is not None and user_count is not None:
                group_rank_str = "`" + str(group_rank) + "`" + "/" + "`" + str(user_count) + "`"
            else:
                group_rank_str = "`?`"
            values = {
                "{item_name}": item_name,
                "{month_name}": datetime.now().strftime("%B"),
                "{player_total_month}": "`" + player_month_total + "`",
                "{global_rank}": global_rank_str,
                "{group_rank}": group_rank_str,
                "{group_total}": "`" + str(group_total) + "`",
                "{user_count}": "`" + str(user_count) + "`",
                "{group_total_month}": "`" + group_month_total + "`",
                "{group_to_group_rank}": "`" + str(group_to_group_rank) + "`" + "/" + "`" + str(total_groups) + "`",
                "{item_id}": str(item_id),
                "{npc_id}": str(npc_id),
                "{npc_name}": npc_name,
                "{kill_count}": str(kill_count),
                "{item_value}": "`" + format_number(total_value) + "`",
                "{quantity}": "`" + str(quantity) + "`",
                "{total_value}": "`" + str(total_value) + "`",
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                # Prefer video for display; keep image_url for attachments/local files.
                "{image_url}": video_url or image_url or "",
                "{video_url}": video_url or "",
                "{video_link}": f"[Video]({video_url})" if video_url else "",
            }
            values.update(self._group_points_placeholder_map(data))
            #print("Sending to replace_placeholders")
            
            embed = replace_placeholders(embed_template, values)
            embed = self._finalize_group_points_embed(embed)
            if group_id == 2:
                embed = await self.remove_group_field(embed)
            if kill_count is None or int(kill_count) < 1:
                embed = await self.remove_kc_field(embed)
            image_url = data.get('image_url', None)
            if image_url and "cdn.discordapp.com" in image_url:
                try:
                    drop = db_session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
                    if drop:
                        image_url = drop.image_url
                except Exception as e:
                    image_url = None
            if image_url:
                try:
                    local_url = image_url.replace("https://www.droptracker.io/", "/store/droptracker/disc/static/assets/")
                    attachment = interactions.File(local_url)
                except Exception as e:
                    print(f"Debug - Couldn't get attachment from path: {e}")
                    attachment = None
                    pass
            #print("Got the embed...")
            # Prefer attaching MP4 if available (Discord renders as native video)
            video_attachment, video_local_path = (None, None)
            if video_url:
                video_attachment, video_local_path = await self._download_video_attachment(video_url, notification.id)

            try:
                if video_attachment:
                    message = await channel.send(content, embed=embed, files=video_attachment)
                elif attachment:
                    message = await channel.send(content, embed=embed, files=attachment)
                else:
                    message = await channel.send(content, embed=embed)
            finally:
                if video_local_path:
                    try:
                        os.remove(video_local_path)
                    except Exception:
                        pass
            
            # Mark as sent
            await self._cleanup_processed_local_video_after_send(db_session, data)
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            
            # Create NotifiedSubmission entry
            from db.models import Drop
            drop = db_session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
            
            if drop and message:
                notified_sub = NotifiedSubmission(
                    channel_id=str(message.channel.id),
                    player_id=player_id,
                    message_id=str(message.id),
                    group_id=group_id,
                    status="sent",
                    drop=drop
                )
                db_session.add(notified_sub)
            
            db_session.commit()
            player_notif_data = data.copy()
            player_notif_data['item_name'] = item_name
            #await self.send_player_notification(player_id, player_name, player_notif_data, 'drop', data.get('drop_id'))
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_drop_notification(self, notification: NotificationQueue, data: dict):
        """Send a drop notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print(f"Got raw drop notification data: {data}")
            drop_id = data.get('drop_id')


            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
            ).first()

            existing_notification = session.query(NotifiedSubmission).filter(
                NotifiedSubmission.player_id == player_id,
                NotifiedSubmission.group_id == group_id,
                NotifiedSubmission.drop_id == drop_id
            ).first()
            if existing_notification:
                print(f"Drop was already notified... Skipping")
                return
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            # Get player name
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            kill_count = data.get('kill_count', None)
            item_id = session.query(ItemList).filter(ItemList.item_name == item_name).first()
            if item_id:
                item_id = item_id.item_id
            else:
                item_id = 1
            npc_name = data.get('npc_name', None)
            if npc_name:
                npc_id = session.query(NpcList).filter(NpcList.npc_name == npc_name).first()
            else:
                npc_id = 0
            if npc_id:
                npc_id = npc_id.npc_id
            else:
                npc_id = 1
            value = data.get('value')
            quantity = data.get('quantity')
            total_value = data.get('total_value')
            image_url = data.get('image_url', None)
            if image_url is None or image_url == "":
                try:
                    drop = session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
                    if drop:
                        image_url = drop.image_url
                except Exception as e:
                    image_url = None
            #print(f"Debug - image_url: {image_url}, type: {type(image_url)}")
            if not image_url or "droptracker.io" not in image_url:
                image_url = ""
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('drop', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('drop', 1)
            #print(f"Debug - embed_template: {embed_template}")
            
            if not embed_template:
                notification.status = 'failed'
                notification.error_message = f"No embed template for group {group_id}"
                session.commit()
                return
            
            # Download image if available
            attachment = None
            if image_url:
                try:
                    # Convert external URL to local path matching the actual storage structure
                    local_url = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_url):
                        attachment = interactions.File(local_url)
                    else:
                        print(f"Debug - Image file not found at: {local_url}")
                        attachment = None
                except Exception as e:
                    print(f"Debug - Couldn't get attachment from path: {e}")
                    attachment = None
                    pass
            
            # Replace placeholders in embed
            player = None
            if not player_id:
                player = session.query(Player).filter(Player.player_name == player_name).first()
                if player:
                    player_id = player.player_id
            
            partition = get_current_partition()
            # Use monthly total computed by redis_updates player cache
            month_total_int = self._get_player_month_total(player_id, partition)
            player_month_total = format_number(month_total_int)
            players_in_group = session.query(Player.player_id).join(Player.groups).filter(Group.group_id == group_id).all()
            group_month_total = format_number(get_player_list_loot_sum([player.player_id for player in players_in_group]))
            # Use centralized rank helper for accuracy
            global_rank_data = loot_tracker.get_player_rank(player_id, None, partition)
            group_rank_data = loot_tracker.get_player_rank(player_id, group_id, partition)
            #print(f"Got group rank data: {group_rank_data}")
            print(f"Got global rank data: {global_rank_data}")
            if group_rank_data:
                group_rank, user_count = group_rank_data
            else:
                group_rank, user_count = None, redis_client.client.zcard(f"leaderboard:{partition}:group:{group_id}")
            if global_rank_data:
                global_rank, total_global_players = global_rank_data
            else:
                global_rank, total_global_players = None, redis_client.client.zcard(f"leaderboard:{partition}")
            # get all group ranks
            all_groups = session.query(Group.group_id).filter(Group.group_id != 2).all()
            #all_groups = session.query(Group.group_id).all()
            total_groups = len(all_groups) - 1
            group_totals = []
            for group in all_groups:
                group_total = redis_client.zsum(f"leaderboard:{partition}:group:{group.group_id}")
                group_totals.append({'id': group.group_id,
                                   'total': group_total})
            sorted_groups = sorted(group_totals, key=lambda x: x['total'], reverse=True)
            group_to_group_rank = str(next((i for i, g in enumerate(sorted_groups) if g['id'] == group_id), 0) + 1)
            formatted_name = get_formatted_name(player_name, group_id, session)
            # Build rank strings safely
            if global_rank is not None and total_global_players is not None:
                global_rank_str = "`" + str(global_rank) + "`" + "/" + "`" + str(total_global_players) + "`"
            else:
                global_rank_str = "`?`"
            if group_rank is not None and user_count is not None:
                group_rank_str = "`" + str(group_rank) + "`" + "/" + "`" + str(user_count) + "`"
            else:
                group_rank_str = "`?`"
            values = {
                "{item_name}": item_name,
                "{month_name}": datetime.now().strftime("%B"),
                "{player_total_month}": "`" + player_month_total + "`",
                "{global_rank}": global_rank_str,
                "{group_rank}": group_rank_str,
                "{group_total}": "`" + str(group_total) + "`",
                "{user_count}": "`" + str(user_count) + "`",
                "{group_total_month}": "`" + group_month_total + "`",
                "{group_to_group_rank}": "`" + str(group_to_group_rank) + "`" + "/" + "`" + str(total_groups) + "`",
                "{item_id}": str(item_id),
                "{npc_id}": str(npc_id),
                "{npc_name}": npc_name,
                "{kill_count}": str(kill_count),
                "{item_value}": "`" + format_number(total_value) + "`",
                "{quantity}": "`" + str(quantity) + "`",
                "{total_value}": "`" + str(total_value) + "`",
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                "{image_url}": image_url or ""
            }
            values.update(self._group_points_placeholder_map(data))
            #print("Sending to replace_placeholders")
            
            embed = replace_placeholders(embed_template, values)
            embed = self._finalize_group_points_embed(embed)
            if group_id == 2:
                embed = await self.remove_group_field(embed)
            if kill_count is None or int(kill_count) < 1:
                embed = await self.remove_kc_field(embed)
            image_url = data.get('image_url', None)
            if image_url and "cdn.discordapp.com" in image_url:
                try:
                    drop = session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
                    if drop:
                        image_url = drop.image_url
                except Exception as e:
                    image_url = None
            if image_url:
                try:
                    local_url = image_url.replace("https://www.droptracker.io/", "/store/droptracker/disc/static/assets/")
                    attachment = interactions.File(local_url)
                except Exception as e:
                    print(f"Debug - Couldn't get attachment from path: {e}")
                    attachment = None
                    pass
            #print("Got the embed...")
            # Send message
            if attachment:
                message = await channel.send(f"{formatted_name} received a drop:", embed=embed, files=attachment)
            else:
                message = await channel.send(f"{formatted_name} received a drop:", embed=embed)
            
            # Mark as sent
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            
            # Create NotifiedSubmission entry
            from db.models import Drop
            drop = session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
            
            if drop and message:
                notified_sub = NotifiedSubmission(
                    channel_id=str(message.channel.id),
                    player_id=player_id,
                    message_id=str(message.id),
                    group_id=group_id,
                    status="sent",
                    drop=drop
                )
                session.add(notified_sub)
            
            session.commit()
            player_notif_data = data.copy()
            player_notif_data['item_name'] = item_name
            #await self.send_player_notification(player_id, player_name, player_notif_data, 'drop', data.get('drop_id'))
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_new_npc_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send notification about new NPC with session"""
        try:
            npc_name = data.get('npc_name')
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            value = data.get('value')
            
            await confirm_new_npc(self.bot, npc_name, player_name, item_name, value)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_new_npc_notification(self, notification: NotificationQueue, data: dict):
        """Send notification about new NPC"""
        try:
            npc_name = data.get('npc_name')
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            value = data.get('value')
            
            await confirm_new_npc(self.bot, npc_name, player_name, item_name, value)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_new_item_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send notification about new item with session"""
        try:
            item_name = data.get('item_name')
            player_name = data.get('player_name')
            item_id = data.get('item_id')
            npc_name = data.get('npc_name')
            value = data.get('value')
            
            await confirm_new_item(self.bot, item_name, player_name, item_id, npc_name, value)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_new_item_notification(self, notification: NotificationQueue, data: dict):
        """Send notification about new item"""
        try:
            item_name = data.get('item_name')
            player_name = data.get('player_name')
            item_id = data.get('item_id')
            npc_name = data.get('npc_name')
            value = data.get('value')
            
            await confirm_new_item(self.bot, item_name, player_name, item_id, npc_name, value)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_name_change_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send notification about player name change with session"""
        try:
            player_name = data.get('player_name')
            player_id = data.get('player_id')
            old_name = data.get('old_name')
            
            await name_change_message(self.bot, player_name, player_id, old_name)
            
            # Also send DM to user if they have Discord ID
            player = db_session.query(Player).filter(Player.player_id == player_id).first()
            if player and player.user:
                user_discord_id = player.user.discord_id
                if user_discord_id:
                    try:
                        user = await self.bot.fetch_user(user_id=user_discord_id)
                        if user:
                            embed = interactions.Embed(
                                title=f"Name change detected:",
                                description=f"Your account, {old_name}, has changed names to {player_name}.",
                                color="#00f0f0"
                            )
                            embed.add_field(
                                name=f"Is this a mistake?",
                                value=f"Reach out in [our discord](https://www.droptracker.io/discord)"
                            )
                            embed.set_footer(global_footer)
                            await user.send(f"Hey, <@{user_discord_id}>", embed=embed)
                    except Exception as e:
                        app_logger.log(log_type="error", data=f"Couldn't DM user about name change: {e}", app_name="notification_service", description="send_name_change_notification")
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_name_change_notification(self, notification: NotificationQueue, data: dict):
        """Send notification about player name change"""
        try:
            player_name = data.get('player_name')
            player_id = data.get('player_id')
            old_name = data.get('old_name')
            
            await name_change_message(self.bot, player_name, player_id, old_name)
            
            # Also send DM to user if they have Discord ID
            player = session.query(Player).filter(Player.player_id == player_id).first()
            if player and player.user:
                user_discord_id = player.user.discord_id
                if user_discord_id:
                    try:
                        user = await self.bot.fetch_user(user_id=user_discord_id)
                        if user:
                            embed = interactions.Embed(
                                title=f"Name change detected:",
                                description=f"Your account, {old_name}, has changed names to {player_name}.",
                                color="#00f0f0"
                            )
                            embed.add_field(
                                name=f"Is this a mistake?",
                                value=f"Reach out in [our discord](https://www.droptracker.io/discord)"
                            )
                            embed.set_footer(global_footer)
                            await user.send(f"Hey, <@{user_discord_id}>", embed=embed)
                    except Exception as e:
                        app_logger.log(log_type="error", data=f"Couldn't DM user about name change: {e}", app_name="notification_service", description="send_name_change_notification")
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_new_player_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send notification about new player with session"""
        try:
            player_name = data.get('player_name')
            
            await new_player_message(self.bot, player_name)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_new_player_notification(self, notification: NotificationQueue, data: dict):
        """Send notification about new player"""
        try:
            player_name = data.get('player_name')
            
            await new_player_message(self.bot, player_name)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_pb_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a personal best notification to Discord with session"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            
            # Get channel ID for this group
            channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_pb'
            ).first()
            pb_id = data.get('pb_id', None)
            if pb_id:
                existing_notification = db_session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.pb_id == pb_id
                ).first()
                if existing_notification:
                    print(f"PB was already notified... Skipping")
                    return
            
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                db_session.commit()
                return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    notification.status = 'failed'
                    notification.error_message = f"No channel configured for group {group_id}"
                    db_session.commit()
                    return
            # hall_of_fame = self.bot.get_ext("services.hall_of_fame")
            # if hall_of_fame:
            #     try:
            #         await hall_of_fame.update_boss_component(group_id, npc_id)
            #     except Exception as e:
            #         print(f"Error updating boss component: {e}")
            #         pass
            # Get data
            player_name = data.get('player_name')
            boss_name = data.get('boss_name')
            time_ms = data.get('time_ms')
            old_time_ms = data.get('old_time_ms')
            kill_time_ms = data.get('kill_time_ms')
            image_url = data.get('image_url')
            team_size = data.get('team_size')
            npc_id = data.get('npc_id')
            # Format times
            time_formatted = convert_from_ms(time_ms)
            old_time_formatted = convert_from_ms(old_time_ms) if old_time_ms else None
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('pb', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('pb', 1)
            if group_id == 2:
                embed_template = await self.remove_group_field(embed_template)
            
            #print(f"Debug - embed_template: {embed_template}")
            partition = get_current_partition()
            player_total_raw = redis_client.client.zscore(f"leaderboard:{partition}", player_id)
            group_wom_id = db_session.query(Group.wom_id).filter(Group.group_id == group_id).first()
            if group_wom_id:
                group_wom_id = group_wom_id[0]
            wom_member_list = []
            if group_wom_id:
                #print("Finding group members?")
                try:
                    wom_member_list = await fetch_group_members(wom_group_id=int(group_wom_id), session_to_use=db_session)
                except Exception as e:
                    #print("Couldn't get the member list", e)
                    return
            player_ids = await associate_player_ids(wom_member_list)
            
            group_ranks = db_session.query(PersonalBestEntry).filter(PersonalBestEntry.player_id.in_(player_ids), PersonalBestEntry.npc_id == int(npc_id),
                                                                        PersonalBestEntry.team_size == team_size).order_by(PersonalBestEntry.personal_best.asc()).all()
            all_ranks = db_session.query(PersonalBestEntry).filter(PersonalBestEntry.npc_id == int(npc_id),
                                                                    PersonalBestEntry.team_size == team_size).order_by(PersonalBestEntry.personal_best.asc()).all()
                #print("Group ranks:",group_ranks)
                #print("All ranks:",all_ranks)
            total_ranked_group = len(group_ranks)
            total_ranked_global = len(all_ranks)
            current_user_best_ms = time_ms
                ## player's rank in group
            group_placement = None
            global_placement = None
            #print("Assembling rankings....")
            ## For some reason, players occassionally don't appear in group rank listings...
            if str(player_id) not in [str(entry.player_id) for entry in group_ranks]:
                # Find where this time would be inserted in the sorted list
                group_placement = len(group_ranks) + 1  # Default to last place (worst time)
                for idx, entry in enumerate(group_ranks, start=1):
                    if current_user_best_ms <= entry.personal_best:
                        # Current user's time is faster or equal, so they rank at this position
                        group_placement = idx
                        break
            else:
                for idx, entry in enumerate(group_ranks, start=1): 
                    if entry.personal_best == current_user_best_ms:
                        group_placement = idx
                        break
            ## player's rank globally
            global_placement = len(all_ranks) + 1  # Default to last place (worst time)
            for idx, entry in enumerate(all_ranks, start=1):
                if current_user_best_ms <= entry.personal_best:
                    global_placement = idx
                    break
            if group_placement is None:
                group_placement = "`?`"
                # Replace placeholders
            formatted_name = get_formatted_name(player_name, group_id, db_session)
            
            replacements = {
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                "{global_rank}": str(global_placement),
                "{total_ranked_global}": str(total_ranked_global),
                "{group_rank}": str(group_placement),
                "{total_ranked_group}": str(total_ranked_group),
                "{npc_name}": boss_name,
                "{npc_id}": str(npc_id),
                "{team_size}": team_size,
                "{personal_best}": time_formatted,
            }
            replacements.update(self._group_points_placeholder_map(data))
            video_url = self._maybe_get_video_url(db_session, data)
            replacements["{video_url}"] = video_url or ""
            replacements["{video_link}"] = f"[Video]({video_url})" if video_url else ""
            # Prefer video for display; keep screenshot in data["image_url"] for attachments
            replacements["{image_url}"] = video_url or (data.get("image_url") or "")
            
            embed = replace_placeholders(embed_template, replacements)
            embed = self._finalize_group_points_embed(embed)
            
            # Send message
            content = f"{formatted_name} has achieved a new personal best:"
            video_attachment, video_local_path = (None, None)
            if video_url:
                video_attachment, video_local_path = await self._download_video_attachment(video_url, notification.id)

            try:
                if video_attachment:
                    message = await channel.send(content, embed=embed, files=video_attachment)
                elif image_url:
                    try:
                        local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                        if os.path.exists(local_path):
                            attachment = interactions.File(local_path)
                            message = await channel.send(content, embed=embed, files=attachment)
                        else:
                            message = await channel.send(content, embed=embed)
                    except Exception:
                        message = await channel.send(content, embed=embed)
                else:
                    message = await channel.send(content, embed=embed)
            finally:
                if video_local_path:
                    try:
                        os.remove(video_local_path)
                    except Exception:
                        pass
            
            await self._cleanup_processed_local_video_after_send(db_session, data)
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_pb_notification(self, notification: NotificationQueue, data: dict):
        """Send a personal best notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_pb'
            ).first()
            pb_id = data.get('pb_id', None)
            if pb_id:
                existing_notification = session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.pb_id == pb_id
                ).first()
                if existing_notification:
                    print(f"PB was already notified... Skipping")
                    return
            
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    notification.status = 'failed'
                    notification.error_message = f"No channel configured for group {group_id}"
                    session.commit()
                    return
            # hall_of_fame = self.bot.get_ext("services.hall_of_fame")
            # if hall_of_fame:
            #     try:
            #         await hall_of_fame.update_boss_component(group_id, npc_id)
            #     except Exception as e:
            #         print(f"Error updating boss component: {e}")
            #         pass
            # Get data
            player_name = data.get('player_name')
            boss_name = data.get('boss_name')
            time_ms = data.get('time_ms')
            old_time_ms = data.get('old_time_ms')
            kill_time_ms = data.get('kill_time_ms')
            image_url = data.get('image_url')
            team_size = data.get('team_size')
            npc_id = data.get('npc_id')
            # Format times
            time_formatted = convert_from_ms(time_ms)
            old_time_formatted = convert_from_ms(old_time_ms) if old_time_ms else None
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('pb', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('pb', 1)
            if group_id == 2:
                embed_template = await self.remove_group_field(embed_template)
            
            #print(f"Debug - embed_template: {embed_template}")
            partition = get_current_partition()
            player_total_raw = redis_client.client.zscore(f"leaderboard:{partition}", player_id)
            group_wom_id = session.query(Group.wom_id).filter(Group.group_id == group_id).first()
            if group_wom_id:
                group_wom_id = group_wom_id[0]
            wom_member_list = []
            if group_wom_id:
                #print("Finding group members?")
                try:
                    wom_member_list = await fetch_group_members(wom_group_id=int(group_wom_id), session_to_use=session)
                except Exception as e:
                    #print("Couldn't get the member list", e)
                    return
            player_ids = await associate_player_ids(wom_member_list)
            
            group_ranks = session.query(PersonalBestEntry).filter(PersonalBestEntry.player_id.in_(player_ids), PersonalBestEntry.npc_id == int(npc_id),
                                                                        PersonalBestEntry.team_size == team_size).order_by(PersonalBestEntry.personal_best.asc()).all()
            all_ranks = session.query(PersonalBestEntry).filter(PersonalBestEntry.npc_id == int(npc_id),
                                                                    PersonalBestEntry.team_size == team_size).order_by(PersonalBestEntry.personal_best.asc()).all()
                #print("Group ranks:",group_ranks)
                #print("All ranks:",all_ranks)
            total_ranked_group = len(group_ranks)
            total_ranked_global = len(all_ranks)
            current_user_best_ms = time_ms
                ## player's rank in group
            group_placement = None
            global_placement = None
            #print("Assembling rankings....")
            ## For some reason, players occassionally don't appear in group rank listings...
            if str(player_id) not in [str(entry.player_id) for entry in group_ranks]:
                # Find where this time would be inserted in the sorted list
                group_placement = len(group_ranks) + 1  # Default to last place (worst time)
                for idx, entry in enumerate(group_ranks, start=1):
                    if current_user_best_ms <= entry.personal_best:
                        # Current user's time is faster or equal, so they rank at this position
                        group_placement = idx
                        break
            else:
                for idx, entry in enumerate(group_ranks, start=1): 
                    if entry.personal_best == current_user_best_ms:
                        group_placement = idx
                        break
            ## player's rank globally
            global_placement = len(all_ranks) + 1  # Default to last place (worst time)
            for idx, entry in enumerate(all_ranks, start=1):
                if current_user_best_ms <= entry.personal_best:
                    global_placement = idx
                    break
            if group_placement is None:
                group_placement = "`?`"
                # Replace placeholders
            formatted_name = get_formatted_name(player_name, group_id, session)
            
            replacements = {
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                "{global_rank}": str(global_placement),
                "{total_ranked_global}": str(total_ranked_global),
                "{group_rank}": str(group_placement),
                "{total_ranked_group}": str(total_ranked_group),
                "{npc_name}": boss_name,
                "{npc_id}": str(npc_id),
                "{team_size}": team_size,
                "{personal_best}": time_formatted,
            }
            replacements.update(self._group_points_placeholder_map(data))
            
            embed = replace_placeholders(embed_template, replacements)
            embed = self._finalize_group_points_embed(embed)
            
            # Send message
            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        message = await channel.send(f"{formatted_name} has achieved a new personal best:", embed=embed, files=attachment)
                    else:
                        #print(f"Debug - PB image file not found at: {local_path}")
                        message = await channel.send(f"{formatted_name} has achieved a new personal best:", embed=embed)
                except Exception as e:
                    #print(f"Debug - Error loading PB attachment: {e}")
                    message = await channel.send(f"{formatted_name} has achieved a new personal best:", embed=embed)
            else:
                message = await channel.send(f"{formatted_name} has achieved a new personal best:", embed=embed)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise

    async def send_pet_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a pet notification to Discord with session"""
        from db.models import NotifiedSubmission
        group_id = notification.group_id
        player_id = notification.player_id
        print("Got pet data:", data)
        pet_name = data.get('pet_name')
        source = data.get('source')
        npc_name = data.get('npc_name')
        killcount = data.get('killcount')
        milestone = data.get('milestone')
        duplicate = data.get('duplicate')
        previously_owned = data.get('previously_owned')
        game_message = data.get('game_message')
        image_url = data.get('image_url')
        item_id = data.get('item_id')
        npc_id = data.get('npc_id')
        is_new_pet = data.get('is_new_pet')
        group_id = data.get('group_id')
        player_name = data.get('player_name')
        update_active = check_active_upgrade(group_id)
        if update_active:
            embed_template = await self.db_ops.get_group_embed('pet', group_id)
        else:
            embed_template = await self.db_ops.get_group_embed('pet', 1)
        
        if not embed_template:
            notification.status = 'failed'
            notification.error_message = f"No embed template for group {group_id}"
            db_session.commit()
            return
        
        
        channel_id_config = db_session.query(GroupConfiguration).filter(
            GroupConfiguration.group_id == group_id,
            GroupConfiguration.config_key == 'channel_id_to_post_pets'
        ).first()
        
        
        if not channel_id_config:
            notification.status = 'failed'
            notification.error_message = f"No channel configured for group {group_id}"
            db_session.commit()
            return
        kc_received = milestone if milestone else killcount
        
        value_dict = {
            "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
            "{pet_name}": pet_name,
            "{source}": source,
            "{npc_name}": npc_name,
            "{killcount}": kc_received, 
            "{milestone}": kc_received,
            "{duplicate}": duplicate,
            "{previously_owned}": previously_owned
        }
        value_dict.update(self._group_points_placeholder_map(data))
        video_url = self._maybe_get_video_url(db_session, data)
        value_dict["{video_url}"] = video_url or ""
        value_dict["{video_link}"] = f"[Video]({video_url})" if video_url else ""
        # Prefer video for display; keep screenshot in data["image_url"] for attachments
        value_dict["{image_url}"] = video_url or (data.get("image_url") or "")
        try:
            channel = await self.bot.fetch_channel(channel_id=channel_id_config.config_value)
            formatted_name = get_formatted_name(player_name, group_id, db_session)
            if channel:
                embed = replace_placeholders(embed_template, value_dict)
                embed = self._finalize_group_points_embed(embed)
                if group_id == 2:
                    embed = await self.remove_group_field(embed)
                
                content = f"{formatted_name} has acquired a new pet!"
                video_attachment, video_local_path = (None, None)
                if video_url:
                    video_attachment, video_local_path = await self._download_video_attachment(video_url, notification.id)

                try:
                    if video_attachment:
                        message = await channel.send(content, embed=embed, files=video_attachment)
                    elif image_url:
                        try:
                            local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                            if os.path.exists(local_path):
                                attachment = interactions.File(local_path)
                                message = await channel.send(content, embed=embed, files=attachment)
                            else:
                                message = await channel.send(content, embed=embed)
                        except Exception:
                            message = await channel.send(content, embed=embed)
                    else:
                        message = await channel.send(content, embed=embed)
                finally:
                    if video_local_path:
                        try:
                            os.remove(video_local_path)
                        except Exception:
                            pass
                
                await self._cleanup_processed_local_video_after_send(db_session, data)
                notification.status = 'sent'
                notification.processed_at = datetime.now()
                db_session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = f"Failed to send pet notification: {e}"
            db_session.commit()
            return

    async def send_pet_notification(self, notification: NotificationQueue, data: dict):
        """Send a pet notification to Discord"""
        from db.models import NotifiedSubmission
        group_id = notification.group_id
        player_id = notification.player_id
        print("Got pet data:", data)
        # notification_data = {
        #         'player_name': player_name,
        #         'player_id': player_id,
        #         'pet_name': pet_name,
        #         'source': source,
        #         'npc_name': npc_name,
        #         'killcount': killcount,
        #         'milestone': milestone,
        #         'duplicate': duplicate,
        #         'previously_owned': previously_owned,
        #         'game_message': game_message,
        #         'image_url': dl_path,
        #         'item_id': pet_item_id,
        #         'npc_id': npc_id,
        #         'is_new_pet': is_new_pet
        #     }
        pet_name = data.get('pet_name')
        source = data.get('source')
        npc_name = data.get('npc_name')
        killcount = data.get('killcount')
        milestone = data.get('milestone')
        duplicate = data.get('duplicate')
        previously_owned = data.get('previously_owned')
        game_message = data.get('game_message')
        image_url = data.get('image_url')
        item_id = data.get('item_id')
        npc_id = data.get('npc_id')
        is_new_pet = data.get('is_new_pet')
        group_id = data.get('group_id')
        player_name = data.get('player_name')
        update_active = check_active_upgrade(group_id)
        if update_active:
            embed_template = await self.db_ops.get_group_embed('pet', group_id)
        else:
            embed_template = await self.db_ops.get_group_embed('pet', 1)
        
        if not embed_template:
            notification.status = 'failed'
            notification.error_message = f"No embed template for group {group_id}"
            session.commit()
            return
        
        
        channel_id_config = session.query(GroupConfiguration).filter(
            GroupConfiguration.group_id == group_id,
            GroupConfiguration.config_key == 'channel_id_to_post_pets'
        ).first()
        
        
        if not channel_id_config:
            notification.status = 'failed'
            notification.error_message = f"No channel configured for group {group_id}"
            session.commit()
            return
        kc_received = milestone if milestone else killcount
        
        value_dict = {
            "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
            "{pet_name}": pet_name,
            "{source}": source,
            "{npc_name}": npc_name,
            "{killcount}": kc_received, 
            "{milestone}": kc_received,
            "{duplicate}": duplicate,
            "{previously_owned}": previously_owned
        }
        value_dict.update(self._group_points_placeholder_map(data))
        try:
            channel = await self.bot.fetch_channel(channel_id=channel_id_config.config_value)
            formatted_name = get_formatted_name(player_name, group_id, session)
            if channel:
                embed = replace_placeholders(embed_template, value_dict)
                embed = self._finalize_group_points_embed(embed)
                if group_id == 2:
                    embed = await self.remove_group_field(embed)
                
                if image_url:
                    try:
                        local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                        if os.path.exists(local_path):
                            attachment = interactions.File(local_path)
                            message = await channel.send(f"{formatted_name} has acquired a new pet!", embed=embed, files=attachment)
                    except Exception as e:
                        message = await channel.send(f"{formatted_name} has acquired a new pet!", embed=embed)
                else:
                    message = await channel.send(f"{formatted_name} has acquired a new pet!", embed=embed)
                
                notification.status = 'sent'
                notification.processed_at = datetime.now()
                session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = f"Failed to send pet notification: {e}"
            session.commit()
            return

    async def send_ca_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a combat achievement notification to Discord with session"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print("Got raw CA data:", data)
            
            # Get channel ID for this group
            channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_ca'
            ).first()
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                db_session.commit()
                return
            
            ca_id = data.get('ca_id', None)
            if ca_id:
                existing_notification = db_session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.ca_id == ca_id
                ).first()
                if existing_notification:
                    print(f"CA was already notified... Skipping")
                    return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    notification.status = 'failed'
                    notification.error_message = f"No channel configured for group {group_id}"
                    db_session.commit()
            # Get data
            player_name = data.get('player_name')
            task_name = data.get('task_name')
            task_tier = data.get('tier')
            image_url = data.get('image_url')
            points_awarded = data.get('points_awarded')
            points_total = data.get('points_total')
            
            # Map tier to color and name
            tier_colors = {
                "1": 0x00ff00,  # Easy - Green
                "2": 0x0000ff,  # Medium - Blue
                "3": 0xff0000,  # Hard - Red
                "4": 0xffff00,  # Elite - Yellow
                "5": 0xff00ff,  # Master - Purple
                "6": 0x00ffff   # Grandmaster - Cyan
            }
            
            tier_names = {
                "1": "Easy",
                "2": "Medium",
                "3": "Hard",
                "4": "Elite",
                "5": "Master",
                "6": "Grandmaster"
            }
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('ca', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('ca', 1)
            
            async with osrs_api.create_client() as client:
                actual_tier = await client.semantic.get_current_ca_tier(points_total)
            #actual_tier = await get_current_ca_tier(points_total)
            tier_order = ['Grandmaster', 'Master', 'Elite', 'Hard', 'Medium', 'Easy']
            if actual_tier is None:
                next_tier = "Easy"
            else:
                next_tier = tier_order[tier_order.index(actual_tier) - 1]
            async with osrs_api.create_client() as client:
                progress, next_tier_points = await client.semantic.get_ca_tier_progress(points_total)
            #progress, next_tier_points = await get_ca_tier_progress(points_total)
            formatted_task_name = task_name.replace(" ", "_").replace("?", "%3F")
            wiki_url = f"https://oldschool.runescape.wiki/w/{formatted_task_name}"
            formatted_task_name = f"[{task_name}]({wiki_url})"
            try:
                if not next_tier_points or next_tier_points == 0:
                    next_tier_points = 38
                points_left = int(next_tier_points) - int(points_total)
            except Exception as e:
                points_left = "Unknown"
            if embed_template:
                value_dict = {
                    "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                    "{task_name}": formatted_task_name,
                    "{current_tier}": actual_tier,
                    "{progress}": progress,
                    "{points_awarded}": points_awarded,
                    "{total_points}": points_total,
                    "{next_tier}": next_tier,
                    "{task_tier}": task_tier,
                    "{next_tier_points}": next_tier_points,
                    "{points_left}": points_left
                }
            value_dict.update(self._group_points_placeholder_map(data))
            video_url = self._maybe_get_video_url(db_session, data)
            value_dict["{video_url}"] = video_url or ""
            value_dict["{video_link}"] = f"[Video]({video_url})" if video_url else ""
            # Prefer video for display; keep screenshot in data["image_url"] for attachments
            value_dict["{image_url}"] = video_url or (data.get("image_url") or "")
            
            embed = replace_placeholders(embed_template, value_dict)
            embed = self._finalize_group_points_embed(embed)
            
            # Send message
            formatted_name = get_formatted_name(player_name, group_id, db_session)
            content = f"{formatted_name} has completed a combat achievement!"
            
            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        message = await channel.send(content, embed=embed, files=attachment)
                    else:
                        #print(f"Debug - CA image file not found at: {local_path}")
                        message = await channel.send(content, embed=embed)
                except Exception as e:
                    #print(f"Debug - Error loading CA attachment: {e}")
                    message = await channel.send(content, embed=embed)
            else:
                message = await channel.send(content, embed=embed)
            # Prefer attaching MP4 if available (Discord renders as native video)
            if video_url:
                video_attachment, video_local_path = await self._download_video_attachment(video_url, notification.id)
                if video_attachment:
                    try:
                        message = await channel.send(content, embed=embed, files=video_attachment)
                    finally:
                        if video_local_path:
                            try:
                                os.remove(video_local_path)
                            except Exception:
                                pass
            
            await self._cleanup_processed_local_video_after_send(db_session, data)
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_ca_notification(self, notification: NotificationQueue, data: dict):
        """Send a combat achievement notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print("Got raw CA data:", data)
            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_ca'
            ).first()
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            ca_id = data.get('ca_id', None)
            if ca_id:
                existing_notification = session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.ca_id == ca_id
                ).first()
                if existing_notification:
                    print(f"CA was already notified... Skipping")
                    return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    notification.status = 'failed'
                    notification.error_message = f"No channel configured for group {group_id}"
                    session.commit()
            # Get data
            player_name = data.get('player_name')
            task_name = data.get('task_name')
            task_tier = data.get('tier')
            image_url = data.get('image_url')
            points_awarded = data.get('points_awarded')
            points_total = data.get('points_total')
            
            # Map tier to color and name
            tier_colors = {
                "1": 0x00ff00,  # Easy - Green
                "2": 0x0000ff,  # Medium - Blue
                "3": 0xff0000,  # Hard - Red
                "4": 0xffff00,  # Elite - Yellow
                "5": 0xff00ff,  # Master - Purple
                "6": 0x00ffff   # Grandmaster - Cyan
            }
            
            tier_names = {
                "1": "Easy",
                "2": "Medium",
                "3": "Hard",
                "4": "Elite",
                "5": "Master",
                "6": "Grandmaster"
            }
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('ca', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('ca', 1)
            
            async with osrs_api.create_client() as client:
                actual_tier = await client.semantic.get_current_ca_tier(points_total)
            #actual_tier = await get_current_ca_tier(points_total)
            tier_order = ['Grandmaster', 'Master', 'Elite', 'Hard', 'Medium', 'Easy']
            if actual_tier is None:
                next_tier = "Easy"
            else:
                next_tier = tier_order[tier_order.index(actual_tier) - 1]
            async with osrs_api.create_client() as client:
                progress, next_tier_points = await client.semantic.get_ca_tier_progress(points_total)
            #progress, next_tier_points = await get_ca_tier_progress(points_total)
            formatted_task_name = task_name.replace(" ", "_").replace("?", "%3F")
            wiki_url = f"https://oldschool.runescape.wiki/w/{formatted_task_name}"
            formatted_task_name = f"[{task_name}]({wiki_url})"
            try:
                if not next_tier_points or next_tier_points == 0:
                    next_tier_points = 38
                points_left = int(next_tier_points) - int(points_total)
            except Exception as e:
                points_left = "Unknown"
            if embed_template:
                value_dict = {
                    "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                    "{task_name}": formatted_task_name,
                    "{current_tier}": actual_tier,
                    "{progress}": progress,
                    "{points_awarded}": points_awarded,
                    "{total_points}": points_total,
                    "{next_tier}": next_tier,
                    "{task_tier}": task_tier,
                    "{next_tier_points}": next_tier_points,
                    "{points_left}": points_left
                }
            value_dict.update(self._group_points_placeholder_map(data))
            
            embed = replace_placeholders(embed_template, value_dict)
            embed = self._finalize_group_points_embed(embed)
            
            # Send message
            formatted_name = get_formatted_name(player_name, group_id, session)
            
            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        message = await channel.send(f"{formatted_name} has completed a combat achievement!", embed=embed, files=attachment)
                    else:
                        #print(f"Debug - CA image file not found at: {local_path}")
                        message = await channel.send(f"{formatted_name} has completed a combat achievement!", embed=embed)
                except Exception as e:
                    #print(f"Debug - Error loading CA attachment: {e}")
                    message = await channel.send(f"{formatted_name} has completed a combat achievement!", embed=embed)
            else:
                message = await channel.send(f"{formatted_name} has completed a combat achievement!", embed=embed)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_clog_notification_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Send a collection log notification to Discord with session"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print(f"Found a collection log notification to send in {group_id}")
            
            # Get channel ID for this group
            channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_clog'
            ).first()
            #print(f"Found a channel id config for {group_id}")
            if not channel_id_config or not channel_id_config.config_value:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                db_session.commit()
                return
            
            clog_id = data.get('clog_id', data.get('log_id', None))
            if clog_id:
        
                existing_notification = db_session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.clog_id == clog_id
                ).first()
                if existing_notification:
                    print(f"Drop was already notified... Skipping")
                    return
            channel = None
            channel_id = channel_id_config.config_value
            if channel_id and channel_id != "" and len(str(channel_id)) > 10:
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = db_session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    #print(f"Invalid channel id: {channel_id}")
                    notification.status = 'failed'
                    notification.error_message = f"Invalid channel id: {channel_id}"
                    db_session.commit()
                    return
            if not channel:
                #print(f"Channel not found for group {group_id} (id was passed as {channel_id})")
                notification.status = 'failed'
                notification.error_message = f"Channel not found for group {group_id}"
                db_session.commit()
                return
            # Get data
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            collection_name = data.get('collection_name')
            image_url = data.get('image_url')
            item_id = data.get('item_id')
            kc = data.get('kc_received')
            npc_name = data.get('npc_name')
            partition = get_current_partition()
            month_total_int = self._get_player_month_total(player_id, partition)
            player_month_total = format_number(month_total_int)
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('clog', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('clog', 1)
            
            if group_id == 2:
                embed_template = await self.remove_group_field(embed_template)

            user_count = format_number(redis_client.client.zcard(f"leaderboard:{partition}:group:{group_id}"))
            # Replace placeholders
            replacements = {
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                "{player_loot_month}": player_month_total,
                "{kc_received}": kc,
                "{item_name}": item_name,
                "{collection_name}": collection_name,
                "{item_id}": item_id,
                "{npc_name}": npc_name,
                "{total_tracked}": user_count
            }
            replacements.update(self._group_points_placeholder_map(data))
            video_url = self._maybe_get_video_url(db_session, data)
            replacements["{video_url}"] = video_url or ""
            replacements["{video_link}"] = f"[Video]({video_url})" if video_url else ""
            # Prefer video for display; keep screenshot in data["image_url"] for attachments
            replacements["{image_url}"] = video_url or (data.get("image_url") or "")
            
            embed = replace_placeholders(embed_template, replacements)
            embed = self._finalize_group_points_embed(embed)
            
            # Send message
            formatted_name = get_formatted_name(player_name, group_id, db_session)
            content = f"{formatted_name} has added an item to their collection log!"
            
            video_attachment, video_local_path = (None, None)
            if video_url:
                video_attachment, video_local_path = await self._download_video_attachment(video_url, notification.id)

            try:
                if video_attachment:
                    message = await channel.send(content, embed=embed, files=video_attachment)
                elif image_url:
                    try:
                        local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                        if os.path.exists(local_path):
                            attachment = interactions.File(local_path)
                            message = await channel.send(content, embed=embed, files=attachment)
                        else:
                            print(f"Debug - Collection log image file not found at: {local_path}")
                            message = await channel.send(content, embed=embed)
                    except Exception as e:
                        print(f"Debug - Error loading collection log attachment: {e}")
                        message = await channel.send(content, embed=embed)
                else:
                    message = await channel.send(content, embed=embed)
            finally:
                if video_local_path:
                    try:
                        os.remove(video_local_path)
                    except Exception:
                        pass
            
            await self._cleanup_processed_local_video_after_send(db_session, data)
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            db_session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            db_session.commit()
            raise

    async def send_clog_notification(self, notification: NotificationQueue, data: dict):
        """Send a collection log notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print(f"Found a collection log notification to send in {group_id}")
            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_clog'
            ).first()
            #print(f"Found a channel id config for {group_id}")
            if not channel_id_config or not channel_id_config.config_value:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            clog_id = data.get('clog_id', data.get('log_id', None))
            if clog_id:
        
                existing_notification = session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.clog_id == clog_id
                ).first()
                if existing_notification:
                    print(f"Drop was already notified... Skipping")
                    return
            channel = None
            channel_id = channel_id_config.config_value
            if channel_id and channel_id != "" and len(str(channel_id)) > 10:
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    #print(f"Invalid channel id: {channel_id}")
                    notification.status = 'failed'
                    notification.error_message = f"Invalid channel id: {channel_id}"
                    session.commit()
                    return
            if not channel:
                #print(f"Channel not found for group {group_id} (id was passed as {channel_id})")
                notification.status = 'failed'
                notification.error_message = f"Channel not found for group {group_id}"
                session.commit()
                return
            # Get data
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            collection_name = data.get('collection_name')
            image_url = data.get('image_url')
            item_id = data.get('item_id')
            kc = data.get('kc_received')
            npc_name = data.get('npc_name')
            partition = get_current_partition()
            month_total_int = self._get_player_month_total(player_id, partition)
            player_month_total = format_number(month_total_int)
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('clog', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('clog', 1)
            
            if group_id == 2:
                embed_template = await self.remove_group_field(embed_template)

            user_count = format_number(redis_client.client.zcard(f"leaderboard:{partition}:group:{group_id}"))
            # Replace placeholders
            replacements = {
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view)",
                "{player_loot_month}": player_month_total,
                "{kc_received}": kc,
                "{item_name}": item_name,
                "{collection_name}": collection_name,
                "{item_id}": item_id,
                "{npc_name}": npc_name,
                "{total_tracked}": user_count
            }
            replacements.update(self._group_points_placeholder_map(data))
            
            embed = replace_placeholders(embed_template, replacements)
            embed = self._finalize_group_points_embed(embed)
            
            # Send message
            formatted_name = get_formatted_name(player_name, group_id, session)
            
            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        message = await channel.send(f"{formatted_name} has added an item to their collection log:", embed=embed, files=attachment)
                    else:
                        print(f"Debug - Collection log image file not found at: {local_path}")
                        message = await channel.send(f"{formatted_name} has added an item to their collection log!", embed=embed)
                except Exception as e:
                    print(f"Debug - Error loading collection log attachment: {e}")
                    message = await channel.send(f"{formatted_name} has added an item to their collection log!", embed=embed)
            else:
                message = await channel.send(f"{formatted_name} has added an item to their collection log!", embed=embed)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_player_notification(self, player_id: int, player_name: str, data: dict, notif_type: str, notif_id: int):
        """Creates an Alert on the website for a player's submission"""
        try:
            xf_user_id = await get_user_id(player_id)
            if xf_user_id:
                link_text = f"View your Profile"
                link_url = f"https://www.droptracker.io/players/{quote(player_name, safe='')}.{player_id}/view"
                if notif_type == 'drop':
                    alert_text = f"Your {data.get('item_name')} drop has been processed & sent to Discord."
                    link_text = f"View Drop"
                    link_url = f"https://www.droptracker.io/drops/{notif_id}"
                elif notif_type == 'clog':
                    alert_text = f"Your new log slot ({data.get('item_name')}) has been processed & sent to Discord."
                elif notif_type == 'ca':
                    alert_text = f"Your {data.get('task_name')} combat achievement has been processed & sent to Discord."
                elif notif_type == 'pb':
                    alert_text = f"Your new personal best ({data.get('npc_name')}) has been processed & sent to Discord."
                else:
                    return

                await create_alert(xf_user_id, alert_text, link_text, link_url)
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error sending player notification: {e}", app_name="notification_service", description="send_player_notification")
            raise

    async def remove_group_field(self, embed: interactions.Embed):
        """Removes the Group field from the embed"""
        if embed.fields:
            embed.fields = [field for field in embed.fields if "Group" not in field.name]
        return embed
    
    async def remove_kc_field(self, embed: interactions.Embed):
        """Removes the Kills field from the embed"""
        if embed.fields:
            embed.fields = [field for field in embed.fields if "Source:" not in field.name]
        return embed
    
    async def _is_not_sent_with_session(self, notification: NotificationQueue, data: dict, db_session):
        """Check if a notification has already been sent by querying the database with a specific session.
        Returns True if the notification should be sent, False if it should be skipped."""
        try:
            from db.models import NotifiedSubmission
            
            # Get the appropriate ID field based on notification type
            id_key = None
            if notification.notification_type == 'drop':
                id_key = 'drop_id'
            elif notification.notification_type == 'pb':
                id_key = 'pb_id'
            elif notification.notification_type == 'ca':
                id_key = 'ca_id'
            elif notification.notification_type == 'clog':
                id_key = 'clog_id'
            else:
                return True  # Allow other notification types to proceed
                
            if not id_key:
                return True
                
            # Get the ID to check
            notification_id = data.get(id_key)
            if not notification_id:
                return True
                
            # Check if this notification has already been sent by querying NotifiedSubmission
            existing_notification = db_session.query(NotifiedSubmission).filter(
                NotifiedSubmission.player_id == notification.player_id,
                NotifiedSubmission.group_id == notification.group_id,
                getattr(NotifiedSubmission, id_key) == notification_id
            ).first()
            
            if existing_notification:
                app_logger.log(log_type="info", 
                             data=f"Notification {notification.id} was already sent for {id_key} {notification_id} in group {notification.group_id}", 
                             app_name="notification_service", 
                             description="_is_not_sent")
                return False  # Return False to prevent sending
            
            return True  # Allow sending
            
        except Exception as e:
            app_logger.log(log_type="error", 
                         data=f"Error checking if notification was sent: {e}", 
                         app_name="notification_service", 
                         description="_is_not_sent")
            return True  # On error, allow sending to be safe

    async def _is_not_sent(self, notification: NotificationQueue, data: dict):
        """Check if a notification has already been sent by querying the database.
        Returns True if the notification should be sent, False if it should be skipped."""
        from api.core import get_db_session
        with get_db_session() as db_session:
            return await self._is_not_sent_with_session(notification, data, db_session)
            
    async def cleanup_tracking_dicts(self):
        """Clean up old NotifiedSubmission entries to prevent database bloat"""
        try:
            from api.core import get_db_session, NotifiedSubmission
            with get_db_session() as db_session:
                # Delete NotifiedSubmission entries older than 30 days
                cutoff_date = datetime.now() - timedelta(days=30)
                old_entries = db_session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.created_at < cutoff_date
                ).all()
                
                if old_entries:
                    for entry in old_entries:
                        db_session.delete(entry)
                    db_session.commit()
                    app_logger.log(log_type="info", 
                                 data=f"Cleaned up {len(old_entries)} old notification entries", 
                                 app_name="notification_service", 
                                 description="cleanup_tracking_dicts")
                
        except Exception as e:
            app_logger.log(log_type="error", 
                         data=f"Error cleaning up old notification entries: {e}", 
                         app_name="notification_service", 
                         description="cleanup_tracking_dicts")

    async def cleanup_stuck_notifications(self):
        """Reset notifications that have been stuck in 'processing' status for too long"""
        try:
            # Find notifications stuck in processing for more than 10 minutes
            stuck_time = datetime.now() - timedelta(minutes=10)
            stuck_notifications = session.query(NotificationQueue).filter(
                NotificationQueue.status == 'processing',
                NotificationQueue.processed_at.is_(None)
            ).all()
            
            if stuck_notifications:
                app_logger.log(log_type="warning", 
                             data=f"Found {len(stuck_notifications)} stuck notifications, resetting to pending", 
                             app_name="notification_service", 
                             description="cleanup_stuck_notifications")
                
                for notification in stuck_notifications:
                    notification.status = 'pending'
                    notification.error_message = 'Reset due to timeout'
                
                session.commit()
                
        except Exception as e:
            app_logger.log(log_type="error", 
                         data=f"Error cleaning up stuck notifications: {e}", 
                         app_name="notification_service", 
                         description="cleanup_stuck_notifications")

    def _get_player_month_total(self, player_id: int, partition: int = None) -> int:
        """Fetch the player's monthly total loot from Redis computed by redis_updates."""
        try:
            if partition is None:
                partition = get_current_partition()
            key = f"player:{player_id}:{partition}:total_loot"
            total_str = redis_client.get(key)
            if total_str is None:
                # Fallback to global leaderboard score if key missing
                score = redis_client.client.zscore(f"leaderboard:{partition}", player_id)
                return int(float(score)) if score is not None else 0
            return int(float(total_str))
        except Exception:
            return 0

""" Helper function to get a list of registered users who are members of a specific group """
async def get_authorized_users(group_id):
    from api.core import get_db_session
    users = []
    db_session = get_db_session()
    try:
        group_config = db_session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id).all()
        # Transform group_config into a dictionary for easy access
        config = {conf.config_key: conf.config_value for conf in group_config}
        if "authed_users" in config:
            authed_users = config["authed_users"]
            if isinstance(authed_users, int):
                authed_users = f"{authed_users}"  # Get the list of authorized user IDs
            authed_users = json.loads(authed_users)
            for authed_id in authed_users:
                user = db_session.query(User).filter(User.discord_id == authed_id).first()
                if user:
                    users.append(user)
    finally:
        try:
            db_session.close()
        except Exception:
            pass
    return users
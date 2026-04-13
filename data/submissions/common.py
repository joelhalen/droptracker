"""Shared utilities and state for submissions processors.

This module centralizes common imports, shared caches, data classes,
and helper functions used by the various submission processors.

All functions/classes are exported with stable names to preserve
backward compatibility with the original `data.submissions` module.
"""

import asyncio
import hashlib
import json
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

from api.core import logger
from db import (
    models,
    CombatAchievementEntry,
    Drop,
    FeatureActivation,
    NotifiedSubmission,
    QuestCompletionEntry,
    PlayerPet,
    session,
    NpcList,
    Player,
    ItemList,
    PersonalBestEntry,
    CollectionLogEntry,
    User,
    Group,
    GroupConfiguration,
    UserConfiguration,
    NotificationQueue,
)
from db.ops import DatabaseOperations, associate_player_ids, get_point_divisor
from sqlalchemy import func, text
from sqlalchemy.engine import Row

from services import redis_updates
from services.points import award_points_to_player

from utils.ge_value import get_true_item_value
from utils import osrs_api
from utils.wiseoldman import (
    check_user_by_id,
    check_user_by_username,
    check_group_by_id,
    fetch_group_members,
    get_collections_logged,
    get_player_boss_kills,
    get_player_metric,
)
from utils.redis import RedisClient
from utils.download import download_player_image, download_image
from utils.format import (
    convert_to_ms,
    format_number,
    get_command_id,
    get_extension_from_content_type,
    get_true_boss_name,
    replace_placeholders,
    convert_from_ms,
    normalize_player_display_equivalence,
)
import interactions
from utils.logger import LoggerClient
from db.app_logger import AppLogger


load_dotenv()
debug_level = "false"
debug = debug_level != "false"


def debug_print(message, **kwargs):
    if debug:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] DEBUG: {message}", **kwargs)


global_footer = os.getenv("DISCORD_MESSAGE_FOOTER")
redis_client = RedisClient()
db = DatabaseOperations()

last_channels_sent = []

app_logger = AppLogger()

# Cache WOM identity lookups to avoid repeated external API waits during bursts.
_wom_user_cache = {}
_WOM_USER_CACHE_TTL_SECONDS = 300

# Caches with size limits to prevent unbounded growth
MAX_CACHE_SIZE = 10000
npc_list = {}
player_list = {}

def _trim_cache(cache: dict, max_size: int = MAX_CACHE_SIZE) -> None:
    """Trim a cache dict to max_size by removing oldest entries (FIFO approximation)."""
    if len(cache) > max_size:
        # Remove roughly 20% of entries when we exceed the limit
        keys_to_remove = list(cache.keys())[: len(cache) - int(max_size * 0.8)]
        for key in keys_to_remove:
            cache.pop(key, None)

def cache_player(player_name: str, player_id: int) -> None:
    """Cache a player name to ID mapping with size limit."""
    _trim_cache(player_list)
    player_list[player_name] = player_id

def cache_npc(npc_name: str, npc_id: int) -> None:
    """Cache an NPC name to ID mapping with size limit."""
    _trim_cache(npc_list)
    npc_list[npc_name] = npc_id


async def get_wom_user_cached(player_name: str):
    now = time.time()
    cached = _wom_user_cache.get(player_name)
    if cached:
        value, ts = cached
        if now - ts < _WOM_USER_CACHE_TTL_SECONDS:
            return value
    try:
        value = await check_user_by_username(player_name)
    except Exception:
        value = (None, None, None, -1)
    _wom_user_cache[player_name] = (value, now)
    if len(_wom_user_cache) > 10000:
        # Trim oldest ~20% to bound memory.
        for key in list(_wom_user_cache.keys())[:2000]:
            _wom_user_cache.pop(key, None)
    return value


def _to_int_or_none(value):
    try:
        if value in (None, "", 0):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_total_level_from_wom_player(wom_player) -> int:
    try:
        overall = wom_player.latest_snapshot.data.skills.get("overall")
        return int(overall.level)
    except Exception:
        return 0


def _apply_authoritative_wom_identity(
    db_session,
    player: Player | None,
    expected_wom_id: int | None,
    *,
    canonical_name: str | None = None,
    total_level: int | None = None,
    log_slots: int | None = None,
    account_hash: str | None = None,
):
    """
    Ensure the local player row reflects WOM's authoritative identity.

    Returns:
        tuple[player|None, bool]: (possibly replaced canonical player, changed_flag)
    """
    if player is None or expected_wom_id is None:
        return player, False

    changed = False
    current_wom_id = _to_int_or_none(getattr(player, "wom_id", None))
    if current_wom_id != expected_wom_id:
        canonical = (
            db_session.query(Player)
            .filter(Player.wom_id == expected_wom_id)
            .first()
        )
        if canonical and canonical.player_id != player.player_id:
            player = canonical
        else:
            player.wom_id = expected_wom_id
            changed = True

    if canonical_name and normalize_player_display_equivalence(player.player_name or "") != normalize_player_display_equivalence(canonical_name):
        player.player_name = canonical_name
        changed = True

    if total_level is not None and int(total_level) > 0 and int(player.total_level or 0) != int(total_level):
        player.total_level = int(total_level)
        changed = True

    if log_slots is not None and int(log_slots) >= 0 and player.log_slots != int(log_slots):
        player.log_slots = int(log_slots)
        changed = True

    if account_hash:
        account_hash = str(account_hash)
        if not player.account_hash:
            existing_hash_owner = (
                db_session.query(Player)
                .filter(Player.account_hash == account_hash)
                .first()
            )
            if (
                existing_hash_owner
                and existing_hash_owner.player_id != player.player_id
                and _to_int_or_none(existing_hash_owner.wom_id) != expected_wom_id
            ):
                existing_hash_owner.account_hash = None
                changed = True
            player.account_hash = account_hash
            changed = True

    return player, changed


class SubmissionResponse:
    """Response object for API submission endpoints.

    Attributes:
        success (bool): Whether the submission was processed successfully.
        message (str): Descriptive message about the submission result.
        notice (str | None): Optional additional notice or warning.
    """

    def __init__(self, success, message, notice=None):
        self.success = success
        self.message = message
        self.notice = notice


class RawDropData:
    """Container class for raw drop submission data."""

    def __init__(self) -> None:
        pass


def check_auth(
    player_name,
    account_hash,
    auth_key,
    external_session=None,
    resolved_player=None,
    resolved_by_name=False,
    expected_wom_id=None,
):
    """Authenticate a player against stored account hash.

    Returns:
        tuple[bool, bool]: (user_exists, authed)
    """

    use_external_session = external_session is not None
    if use_external_session:
        db_session = external_session
    else:
        db_session = session
    try:
        player = resolved_player
        if player is None:
            player = db_session.query(Player).filter(Player.player_name.ilike(player_name)).first()

        if not player:
            return False, False

        if player.account_hash:
            if str(account_hash) != str(player.account_hash):
                return True, False
            else:
                return True, True
        else:
            # Never bind an account hash to a name-resolved row unless WOM identity was verified.
            if resolved_by_name:
                if expected_wom_id is None:
                    app_logger.log(
                        log_type="warning",
                        data=f"Rejected account hash bind for {player_name}: missing expected WOM identity",
                        app_name="core",
                        description="check_auth",
                    )
                    return True, False
                try:
                    expected_wom_id = int(expected_wom_id)
                except (TypeError, ValueError):
                    return True, False
                if int(player.wom_id or 0) != expected_wom_id:
                    app_logger.log(
                        log_type="warning",
                        data=(
                            f"Rejected account hash bind for {player_name}: "
                            f"row WOM {player.wom_id} != expected WOM {expected_wom_id}"
                        ),
                        app_name="core",
                        description="check_auth",
                    )
                    return True, False

            existing_player = (
                db_session.query(Player).filter(Player.account_hash == account_hash).first()
                if account_hash
                else None
            )
            if existing_player:
                if existing_player.player_id != player.player_id:
                    if expected_wom_id is not None:
                        try:
                            expected_wom_id = int(expected_wom_id)
                        except (TypeError, ValueError):
                            return True, False
                        if int(existing_player.wom_id or 0) == expected_wom_id:
                            return True, True
                    return True, False
                if (
                    normalize_player_display_equivalence(existing_player.player_name)
                    != normalize_player_display_equivalence(player_name)
                ):
                    existing_player.player_name = player_name
                    app_logger.log(
                        log_type="access",
                        data=f"Player {player_name} already exists with account hash {account_hash}, updating player name to {player_name}",
                        app_name="core",
                        description="check_auth",
                    )
                    try:
                        db_session.commit()
                    except Exception as e:
                        debug_print("Error committing player name change:" + str(e))
                        db_session.rollback()
            player.account_hash = str(account_hash)
            try:
                db_session.commit()
            except Exception as e:
                debug_print("Error committing player name change:" + str(e))
                db_session.rollback()
            return True, True
    except Exception as e:
        debug_print("Error checking auth:" + str(e))
        return False, False


def select_session_and_flag(external_session):
    """Return (session, use_external_session_flag).
    
    When no external session is provided, uses the global scoped session.
    The scoped session should be cleaned up via session.remove() after each request.
    """
    if external_session is not None:
        return external_session, True
    # Use the global scoped session - it will be cleaned up at request end
    return session, False


async def ensure_item_for_drop(session, item_id, item_name):
    """Ensure an item exists by id or name. Mirrors drop processor behavior."""

    item = None
    if item_id is not None:
        item = session.query(ItemList).filter(ItemList.item_id == item_id).first()
    if not item and item_name is not None:
        # Release any open transaction before awaiting external API calls.
        # Otherwise the Session can keep a pooled DB connection checked out
        # while waiting on network I/O.
        try:
            session.rollback()
        except Exception:
            pass
        try:
            async with osrs_api.create_client() as client:
                real_item = await client.semantic.check_item_exists(item_name)
            if real_item and item_id is not None:
                item = ItemList(item_name=item_name, item_id=item_id, noted=0, stackable=0, stacked=0)
                session.add(item)
                session.commit()
        except Exception:
            return None
    return item


async def screenshot_required(session, group_id) -> bool:
    """ Checks whether a group has configured that screenshots must be included for notifications to be created """
    config_key = "only_send_messages_with_images"
    config: GroupConfiguration = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id,
                                                      GroupConfiguration.config_key == config_key).first()
    if config:
        # "1" or "true" means screenshots ARE required; "0" or "false" means NOT required
        if config.config_value == "1" or config.config_value.lower() == "true":
            return True
        return False
    return False

_group_point_system_cache = {}
_GROUP_POINT_CACHE_TTL = 60

def check_group_point_system_active(group_id, external_session=None):
    now = time.time()
    cached = _group_point_system_cache.get(group_id)
    if cached is not None:
        value, ts = cached
        if now - ts < _GROUP_POINT_CACHE_TTL:
            return value

    session, use_external_session = select_session_and_flag(external_session)
    premium_status = session.execute(text("SELECT 1 FROM xenforo.xf_dt_group_upgrade_active WHERE group_id = :group_id AND is_cancelled = 0 AND group_upgrade_id >= 2 LIMIT 1"), {"group_id": group_id}).first()
    result = premium_status is not None
    _group_point_system_cache[group_id] = (result, now)
    return result
    

async def ensure_player_and_auth(session, player_name, account_hash, auth_key):
    """Ensure player exists, cache id, then auth. Returns (player, authed, user_exists)."""

    player_name = str(player_name).strip() if player_name is not None else ""
    account_hash = str(account_hash) if account_hash is not None else ""
    if not player_name:
        return None, False, False

    await asyncio.sleep(0)

    # 1) Deterministic local lookups.
    player_by_hash = None
    if account_hash:
        player_by_hash = session.query(Player).filter(Player.account_hash == account_hash).first()

    player_by_name_fast = session.query(Player).filter(Player.player_name == player_name).first()
    player = player_by_hash or player_by_name_fast
    resolved_by_name = False
    expected_wom_id = None
    wom_log_slots = None
    wom_total_level = None

    # 2) Authoritative WOM identity lookup for this RSN.
    wom_player = None
    canonical_name = player_name
    try:
        # Release any open transaction before awaiting external API.
        try:
            session.rollback()
        except Exception:
            pass
        wom_player, resolved_name, wom_player_id, log_slots = await get_wom_user_cached(player_name)
        if wom_player and wom_player_id not in (None, "", 0):
            expected_wom_id = int(wom_player_id)
            wom_log_slots = log_slots
            canonical_name = str(resolved_name or player_name)
            wom_total_level = _extract_total_level_from_wom_player(wom_player)
    except (TypeError, ValueError):
        expected_wom_id = None

    if expected_wom_id is not None:
        # Prefer canonical WOM row over hash/name-resolved rows.
        player_by_wom = session.query(Player).filter(Player.wom_id == expected_wom_id).first()
        if player_by_wom:
            player = player_by_wom
        elif player is None:
            player = player_by_name_fast
            resolved_by_name = player is not None

        if player is None:
            # No local row matched: create canonical row from WOM identity.
            total_level = wom_total_level if wom_total_level is not None else 0
            player = Player(
                wom_id=expected_wom_id,
                player_name=canonical_name,
                account_hash=account_hash if account_hash else None,
                total_level=total_level,
                log_slots=wom_log_slots if wom_log_slots is not None else 0,
            )
            try:
                session.add(player)
                session.commit()
            except Exception:
                session.rollback()
                player = session.query(Player).filter(Player.wom_id == expected_wom_id).first()
                if player is None:
                    return None, False, False
        else:
            player, changed = _apply_authoritative_wom_identity(
                session,
                player,
                expected_wom_id,
                canonical_name=canonical_name,
                total_level=wom_total_level,
                log_slots=wom_log_slots,
                account_hash=account_hash if account_hash else None,
            )
            if changed:
                try:
                    session.commit()
                except Exception as e:
                    debug_print("Error committing WOM identity reconciliation: " + str(e))
                    session.rollback()

        # Hash conflicts are still treated as auth failures.
        if account_hash and player and player.account_hash and str(player.account_hash) != account_hash:
            logger.log_sync(
                "warning",
                f"ensure_player_and_auth: hash mismatch for WOM {expected_wom_id}; refusing auth for {player_name}",
            )
            player_list[player_name] = player.player_id
            return player, False, True
    elif not player:
        # WOM unavailable + no deterministic local match.
        logger.log_sync(
            "warning",
            f"ensure_player_and_auth: WOM lookup unavailable for {player_name}; refusing name-only bind",
        )
        return None, False, False

    if not player:
        return None, False, False

    if expected_wom_id is not None:
        # Keep name/log slots aligned for the canonical WOM row.
        desired_name = canonical_name or player_name
        if normalize_player_display_equivalence(player.player_name or "") != normalize_player_display_equivalence(desired_name):
            player.player_name = desired_name
        if wom_log_slots is not None and wom_log_slots >= 0 and player.log_slots != wom_log_slots:
            player.log_slots = wom_log_slots
        if wom_total_level is not None and int(wom_total_level) > 0 and int(player.total_level or 0) != int(wom_total_level):
            player.total_level = int(wom_total_level)
        try:
            session.commit()
        except Exception:
            session.rollback()

    player_list[player_name] = player.player_id
    await asyncio.sleep(0)
    user_exists, authed = check_auth(
        player_name,
        account_hash,
        auth_key,
        session,
        resolved_player=player,
        resolved_by_name=resolved_by_name,
        expected_wom_id=expected_wom_id,
    )

    return player, authed, user_exists


unique_id_cache = {"clog": [], "drop": [], "pb": [], "ca": [], "pet": [], "quest": []}


async def ensure_can_create(session, unique_id, submission_type) -> bool:
    """Ensure no duplicate recent submission exists for a unique_id.

    Returns:
        bool: True if safe to create, False if duplicate exists.
    """

    await asyncio.sleep(0)
    if submission_type not in unique_id_cache:
        unique_id_cache[submission_type] = []
    if unique_id in unique_id_cache[submission_type]:
        return False
    unique_id_cache[submission_type].append(unique_id)
    if len(unique_id_cache[submission_type]) > 1000:
        unique_id_cache[submission_type].pop(0)
    
    def _check_existing():
        cutoff = datetime.now() - timedelta(hours=1)
        match submission_type:
            case "clog":
                return session.query(CollectionLogEntry).filter(
                    CollectionLogEntry.unique_id == unique_id,
                    CollectionLogEntry.date_added > cutoff,
                ).first()
            case "drop":
                return session.query(Drop).filter(
                    Drop.unique_id == unique_id,
                    Drop.used_api == True,
                    Drop.date_added > cutoff,
                ).first()
            case "pb":
                return session.query(PersonalBestEntry).filter(
                    PersonalBestEntry.unique_id == unique_id,
                    PersonalBestEntry.date_added > cutoff,
                ).first()
            case "ca":
                return session.query(CombatAchievementEntry).filter(
                    CombatAchievementEntry.unique_id == unique_id,
                    CombatAchievementEntry.date_added > cutoff,
                ).first()
            case "pet":
                return session.query(PlayerPet).filter(
                    PlayerPet.unique_id == unique_id,
                    PlayerPet.date_added > cutoff,
                ).first()
            case "quest":
                return session.query(QuestCompletionEntry).filter(
                    QuestCompletionEntry.unique_id == unique_id,
                    QuestCompletionEntry.date_added > cutoff,
                ).first()
        return None
    
    existing_entry = _check_existing()
    return existing_entry is None


async def ensure_npc_id_for_player(session, npc_name, player_id, player_name, use_external_session):
    """Resolve npc_id using cache, DB, or create via external API, else queue notification."""

    if not npc_name:
        return None, None
    if npc_name in npc_list:
        return npc_list[npc_name], npc_name
    if ("doom of mokhaiotl" in npc_name.lower()) and ("(level" in npc_name.lower()):
        import re

        match = re.search(r"\(\s*Level\s*:??\s*(\d+)\s*\)", npc_name, flags=re.IGNORECASE)
        level_value = None
        if match:
            #print("Got a match on doom level value:", match.group(1))
            level_value = int(match.group(1))
            try:
                level_value = int(level_value)
            except Exception:
                return 14704, npc_name
            npc_name = re.sub(r"\(\s*Level\s*:??\s*(\d+)\s*\)", r"(Level \1)", npc_name, flags=re.IGNORECASE)
            #print("Parsed doom's name:", npc_name, "Level:", level_value)
            return (14707 + level_value), npc_name
        return 14707, npc_name
    npc_row = session.query(NpcList.npc_id).filter(NpcList.npc_name == npc_name).first()
    if npc_row:
        npc_list[npc_name] = npc_row.npc_id
        return npc_row.npc_id, npc_name
    player_id = player_list.get(player_name)
    if player_id == 0:
        return None, npc_name
    try:
        async with osrs_api.create_client() as client:
            npc_id = await client.semantic.get_npc_id(npc_name)
        if npc_id:
            new_npc = NpcList(npc_id=npc_id, npc_name=npc_name)
            session.add(new_npc)
            session.commit()
            npc_list[npc_name] = npc_id
            return npc_id, npc_name
    except Exception:
        pass
    notification_data = {"npc_name": npc_name, "player_name": player_name, "player_id": player_id}
    await create_notification(
        "new_npc",
        player_id,
        notification_data,
        existing_session=session if use_external_session else None,
    )
    return None, npc_name


def resolve_attachment_from_drop_data(drop_data):
    """Return (attachment_url, attachment_type) based on drop_data."""

    downloaded = drop_data.get("downloaded", False)
    image_url = drop_data.get("image_url", None)
    if downloaded:
        return image_url, "downloaded"
    if drop_data.get("attachment_type", None) is not None:
        return drop_data.get("attachment_url", None), drop_data.get("attachment_type", None)
    return image_url, None


def get_player_groups_with_global(session, player: Player):
    """Fetch groups via association table, ensure global group membership."""

    player_gids = session.execute(
        text("SELECT group_id FROM user_group_association WHERE player_id = :player_id"),
        {"player_id": player.player_id},
    ).all()

    group_ids = {gid[0] for gid in player_gids} if player_gids else set()
    global_group = session.query(Group).filter(Group.group_id == 2).first()

    if global_group and global_group.group_id not in group_ids:
        player.add_group(global_group)
        session.commit()
        group_ids.add(global_group.group_id)

    if not group_ids:
        return []

    player_groups = session.query(Group).filter(Group.group_id.in_(group_ids)).all()
    return player_groups


def is_truthy_config(value):
    if value is None:
        return False
    v = str(value).strip().lower()
    return v == "true" or v == "1"


def get_group_drop_notify_settings(session, group_id):
    """Return (min_value_to_notify:int, send_stacks:bool)."""

    min_value_config = (
        session.query(GroupConfiguration)
        .filter(
            GroupConfiguration.group_id == group_id,
            GroupConfiguration.config_key == "minimum_value_to_notify",
        )
        .first()
    )
    min_value_to_notify = int(min_value_config.config_value) if min_value_config else 2500000
    should_send_stacks = (
        session.query(GroupConfiguration)
        .filter(
            GroupConfiguration.group_id == group_id,
            GroupConfiguration.config_key == "send_stacks_of_items",
        )
        .first()
    )
    send_stacks = is_truthy_config(should_send_stacks.config_value) if should_send_stacks else False
    return min_value_to_notify, send_stacks


def is_user_dm_enabled(session, user_id, key):
    cfg = (
        session.query(UserConfiguration)
        .filter(UserConfiguration.user_id == user_id, UserConfiguration.config_key == key)
        .first()
    )
    return is_truthy_config(cfg.config_value) if cfg else False


async def ensure_item_by_name(session, item_name):
    if not item_name:
        return None
    item = session.query(ItemList).filter(ItemList.item_name == item_name).first()
    if item:
        return item
    try:
        async with osrs_api.create_client() as client:
            item_id = await client.semantic.get_item_id(item_name)
        if item_id:
            item = ItemList(item_name=item_name, item_id=item_id, noted=0, stackable=0, stacked=0)
            session.add(item)
            session.commit()
            return item
    except Exception:
        return None
    return None


async def ensure_player_by_name_then_auth(session, player_name, account_hash, auth_key):
    # Use the canonical identity flow so WOM stays source-of-truth for RSN -> WOM ID.
    return await ensure_player_and_auth(session, player_name, account_hash, auth_key)


stored_notifications = {}
recently_sent = []


async def create_notification(notification_type, player_id, data, group_id=None, existing_session=None):
    """Create a notification queue entry."""

    global stored_notifications
    try:
        debug_test = int(player_id) == 1 if player_id is not None else False
    except (TypeError, ValueError):
        debug_test = False
    if group_id is not None:
        if group_id not in stored_notifications:
            stored_notifications[group_id] = []
    else:
        if 0 not in stored_notifications:
            stored_notifications[0] = []
        group_id = 0
    if len(stored_notifications[group_id]) > 100:
        while len(stored_notifications[group_id]) > 100:
            stored_notifications[group_id].pop()
    use_existing_session = existing_session is not None
    if use_existing_session:
        db_session = existing_session
    else:
        db_session = session
    hashed_data = hashlib.sha256(json.dumps(data).encode()).hexdigest()
    if debug_test:
        await log_to_file(f"hashed data: {hashed_data}")
    if hashed_data in stored_notifications[group_id]:
        if debug_test:
            await log_to_file(
                f"Debug test: Notification already exists for group {group_id}, returning from create_notification without creation"
            )
            await log_to_file(f"Existing hashed data: {stored_notifications[group_id]}")
        return
    stored_notifications[group_id].append(hashed_data)
    notification = NotificationQueue(
        notification_type=notification_type,
        player_id=player_id,
        data=json.dumps(data),
        group_id=group_id if group_id != 0 else None,
        status="pending",
    )
    
    db_session.add(notification)
    if not use_existing_session:
        db_session.commit()
    notification_id = notification.id
    if debug_test:
        await log_to_file(f"Debug test: Created notification for group {group_id}: {notification_id}")
    return notification_id


async def create_player(player_name, account_hash, existing_session=None):
    """Create or reconcile a player using WOM-authoritative identity."""

    use_existing_session = existing_session is not None
    if use_existing_session:
        db_session = existing_session
    else:
        db_session = session

    account_hash = str(account_hash) if account_hash is not None else ""
    if not account_hash or len(account_hash) < 5:
        debug_print("Account hash is too short, aborting")
        return False

    player_name = str(player_name).strip() if player_name is not None else ""
    if not player_name:
        return None

    # Always resolve via WOM first so RSN -> WOM ID is authoritative.
    wom_player, resolved_name, wom_player_id, log_slots = await get_wom_user_cached(player_name)
    if not wom_player or wom_player_id in (None, "", 0):
        debug_print(f"WOM lookup failed for {player_name}; refusing non-authoritative create")
        return None

    try:
        expected_wom_id = int(wom_player_id)
    except (TypeError, ValueError):
        return None

    canonical_name = str(resolved_name or player_name)
    wom_total_level = _extract_total_level_from_wom_player(wom_player)
    player = db_session.query(Player).filter(Player.wom_id == expected_wom_id).first()
    if not player:
        # Fallback to existing local identity rows and reconcile to authoritative WOM ID.
        player = db_session.query(Player).filter(Player.account_hash == account_hash).first()
    if not player:
        player = db_session.query(Player).filter(Player.player_name == player_name).first()

    if player is not None:
        old_name = player.player_name
        player, changed = _apply_authoritative_wom_identity(
            db_session,
            player,
            expected_wom_id,
            canonical_name=canonical_name,
            total_level=wom_total_level,
            log_slots=log_slots,
            account_hash=account_hash,
        )
        if changed:
            try:
                db_session.commit()
            except Exception as e:
                db_session.rollback()
                debug_print("Error committing reconciled player row: " + str(e))
                return None

            if (
                old_name
                and normalize_player_display_equivalence(old_name)
                != normalize_player_display_equivalence(player.player_name)
            ):
                notification_data = {
                    "player_name": player.player_name,
                    "player_id": player.player_id,
                    "old_name": old_name,
                }
                if player.user:
                    user = db_session.query(User).filter(User.user_id == player.user_id).first()
                    if user:
                        should_dm_cfg = (
                            db_session.query(UserConfiguration)
                            .filter(
                                UserConfiguration.user_id == user.user_id,
                                UserConfiguration.config_key == "dm_account_changes",
                            )
                            .first()
                        )
                        if should_dm_cfg:
                            should_dm = str(should_dm_cfg.config_value).lower()
                            should_dm = True if should_dm in ("true", "1") else False
                            if should_dm:
                                await create_notification(
                                    "dm_name_change",
                                    player.player_id,
                                    notification_data,
                                    existing_session=db_session if use_existing_session else None,
                                )

                await create_notification(
                    "name_change",
                    player.player_id,
                    notification_data,
                    existing_session=db_session if use_existing_session else None,
                )
    else:
        try:
            overall = wom_player.latest_snapshot.data.skills.get("overall")
            total_level = overall.level
        except Exception:
            total_level = 0

        new_player = Player(
            wom_id=expected_wom_id,
            player_name=canonical_name,
            account_hash=account_hash,
            total_level=total_level,
            log_slots=log_slots if log_slots is not None else 0,
        )
        db_session.add(new_player)
        try:
            db_session.commit()
        except Exception:
            db_session.rollback()
            player = db_session.query(Player).filter(Player.wom_id == expected_wom_id).first()
            if not player:
                return None
        else:
            player = new_player
            app_logger.log(
                log_type="access",
                data=f"{canonical_name} has been created with ID {new_player.player_id} (hash: {account_hash}) ",
                app_name="core",
                description="create_player",
            )
            notification_data = {
                "player_name": canonical_name,
                "wom_id": expected_wom_id,
                "player_id": new_player.player_id,
                "account_hash": account_hash,
            }
            await create_notification(
                "new_player",
                new_player.player_id,
                notification_data,
                existing_session=db_session if use_existing_session else None,
            )

    player_list[player_name] = player.player_id
    if canonical_name:
        player_list[canonical_name] = player.player_id
    return player


async def try_create_player(bot: interactions.Client, player_name, account_hash):
    account_hash = str(account_hash)
    if not account_hash or len(account_hash) < 5:
        return False
    resolved_name = str(player_name).strip() if player_name is not None else ""
    player = await create_player(resolved_name, account_hash, existing_session=session)
    if not player:
        return None

    normalized_old = normalize_player_display_equivalence(resolved_name)
    normalized_new = normalize_player_display_equivalence(player.player_name or "")
    if normalized_old and normalized_new and normalized_old != normalized_new:
        old_name = resolved_name
        new_name = player.player_name
        if player.user:
            user: User = player.user
            user_discord_id = user.discord_id
            if user_discord_id:
                try:
                    user = await bot.fetch_user(user_id=user_discord_id)
                    if user:
                        embed = interactions.Embed(
                            title="Name change detected:",
                            description=f"Your account, {old_name}, has changed names to {new_name}.",
                            color="#00f0f0",
                        )
                        embed.add_field(
                            name="Is this a mistake?",
                            value="Reach out in [our discord](https://www.droptracker.io/discord)",
                        )
                        embed.set_footer(global_footer)
                        await user.send(f"Hey, <@{user.discord_id}>", embed=embed)
                except Exception as e:
                    debug_print("Couldn't DM the user on a name change:" + str(e))
        from utils.messages import name_change_message

        await name_change_message(bot, new_name, player.player_id, old_name)

    player_list[resolved_name] = player.player_id
    if player.player_name:
        player_list[player.player_name] = player.player_id
    return player


async def log_to_file(data):
    file_path = "data/logs/debug_test.log"
    try:
        with open(file_path, "a") as file:
            file.write(data + "\n")
    except Exception as e:
        debug_print("Couldn't log to file: " + str(e))
        app_logger.log(log_type="error", data=f"Couldn't log to file: {e}", app_name="core", description="log_to_file")



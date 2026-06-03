"""
Read-only database inspection tools for the DropTracker AI support bot.

All functions here perform SELECT-only queries — no inserts, updates, or deletes.
Tool definitions follow the Anthropic tool use API schema format.
"""

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text

from db.models import (
    session,
    Group,
    GroupConfiguration,
    ItemList,
    NpcList,
    PersonalBestEntry,
    Player,
)


# ---------------------------------------------------------------------------
# Tool schemas (Anthropic tool use format)
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS = [
    {
        "name": "lookup_player",
        "description": (
            "Look up an OSRS player by username in the DropTracker database. "
            "Returns the player's basic info, total level, collection log slots, "
            "WOM ID, and which groups they belong to."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "player_name": {
                    "type": "string",
                    "description": "The exact or approximate OSRS player username",
                }
            },
            "required": ["player_name"],
        },
    },
    {
        "name": "lookup_group",
        "description": (
            "Look up a DropTracker group (clan) by name or numeric group ID. "
            "Returns the group's name, description, member count, WOM ID, and invite URL."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "identifier": {
                    "type": "string",
                    "description": "Group name (partial match accepted) or numeric group ID",
                }
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "get_player_recent_drops",
        "description": (
            "Get the most recent item drops for a player from the current month's partition. "
            "Returns item name, source NPC, value, quantity, and date."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "player_name": {
                    "type": "string",
                    "description": "The OSRS player username",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of drops to return (1–10, default 5)",
                    "default": 5,
                },
            },
            "required": ["player_name"],
        },
    },
    {
        "name": "get_group_members",
        "description": (
            "Get the list of players in a group/clan. "
            "Returns member names and total levels (up to 50 members shown)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "group_identifier": {
                    "type": "string",
                    "description": "Group name or numeric group ID",
                }
            },
            "required": ["group_identifier"],
        },
    },
    {
        "name": "search_items",
        "description": (
            "Search for OSRS items in the DropTracker item database by name. "
            "Returns up to 10 matching items with their IDs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "item_name": {
                    "type": "string",
                    "description": "Item name or partial name to search for",
                }
            },
            "required": ["item_name"],
        },
    },
    {
        "name": "get_system_overview",
        "description": (
            "Get a high-level overview of the DropTracker system: "
            "total registered players, total groups, and operational status."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_player_personal_bests",
        "description": (
            "Get a player's personal best boss kill times recorded in DropTracker. "
            "Returns boss names, PB times, and team sizes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "player_name": {
                    "type": "string",
                    "description": "The OSRS player username",
                }
            },
            "required": ["player_name"],
        },
    },
    {
        "name": "get_group_config",
        "description": (
            "Get the public configuration settings for a group: "
            "minimum drop value threshold, notification preferences, etc."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "group_identifier": {
                    "type": "string",
                    "description": "Group name or numeric group ID",
                }
            },
            "required": ["group_identifier"],
        },
    },
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_player(player_name: str):
    return (
        session.query(Player)
        .filter(Player.player_name.ilike(player_name))
        .first()
    )


def _find_group(identifier: str):
    if identifier.strip().isdigit():
        return session.query(Group).filter_by(group_id=int(identifier)).first()
    return (
        session.query(Group)
        .filter(Group.group_name.ilike(f"%{identifier}%"))
        .first()
    )


def _ms_to_time_str(ms: int) -> str:
    minutes = ms // 60000
    seconds = (ms % 60000) // 1000
    centiseconds = (ms % 1000) // 10
    return f"{minutes}:{seconds:02d}.{centiseconds:02d}"


# ---------------------------------------------------------------------------
# Tool implementations (read-only)
# ---------------------------------------------------------------------------

def lookup_player(player_name: str) -> dict:
    try:
        player = _find_player(player_name)
        if not player:
            return {"found": False, "message": f"No player found matching '{player_name}'."}

        groups = [g.group_name for g in player.groups] if player.groups else []
        return {
            "found": True,
            "player_name": player.player_name,
            "player_id": player.player_id,
            "total_level": player.total_level,
            "log_slots": player.log_slots,
            "wom_id": player.wom_id,
            "groups": groups,
            "hidden": player.hidden,
            "date_added": str(player.date_added) if player.date_added else None,
            "date_updated": str(player.date_updated) if player.date_updated else None,
        }
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


def lookup_group(identifier: str) -> dict:
    try:
        group = _find_group(identifier)
        if not group:
            return {"found": False, "message": f"No group found for '{identifier}'."}

        try:
            player_count = len(list(group.players))
        except Exception:
            player_count = 0

        return {
            "found": True,
            "group_id": group.group_id,
            "group_name": group.group_name,
            "description": group.description,
            "wom_id": group.wom_id,
            "invite_url": group.invite_url,
            "player_count": player_count,
            "date_added": str(group.date_added) if group.date_added else None,
        }
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


def get_player_recent_drops(player_name: str, limit: int = 5) -> dict:
    try:
        limit = max(1, min(limit, 10))
        player = _find_player(player_name)
        if not player:
            return {"found": False, "message": f"No player found matching '{player_name}'."}

        now = datetime.now()
        partition = now.year * 100 + now.month
        table_name = f"drops_{partition}"

        rows = session.execute(
            text(
                f"SELECT item_id, npc_id, value, quantity, date_added "
                f"FROM {table_name} "
                f"WHERE player_id = :pid "
                f"ORDER BY date_added DESC "
                f"LIMIT :lim"
            ),
            {"pid": player.player_id, "lim": limit},
        ).fetchall()

        drops = []
        for row in rows:
            item = session.query(ItemList).filter_by(item_id=row.item_id).first()
            npc = (
                session.query(NpcList).filter_by(npc_id=row.npc_id).first()
                if row.npc_id
                else None
            )
            drops.append(
                {
                    "item": item.item_name if item else f"Item #{row.item_id}",
                    "npc": npc.npc_name if npc else (f"NPC #{row.npc_id}" if row.npc_id else "Unknown"),
                    "value": row.value,
                    "quantity": row.quantity,
                    "date": str(row.date_added),
                }
            )

        return {
            "found": True,
            "player_name": player.player_name,
            "partition": table_name,
            "drops": drops,
        }
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


def get_group_members(group_identifier: str) -> dict:
    try:
        group = _find_group(group_identifier)
        if not group:
            return {"found": False, "message": f"No group found for '{group_identifier}'."}

        try:
            players = list(group.players)
        except Exception:
            players = []

        members = [
            {"name": p.player_name, "total_level": p.total_level}
            for p in players[:50]
        ]

        return {
            "found": True,
            "group_name": group.group_name,
            "total_member_count": len(players),
            "members_shown": len(members),
            "members": members,
        }
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


def search_items(item_name: str) -> dict:
    try:
        items = (
            session.query(ItemList)
            .filter(ItemList.item_name.ilike(f"%{item_name}%"))
            .limit(10)
            .all()
        )
        if not items:
            return {"found": False, "message": f"No items found matching '{item_name}'."}

        return {
            "found": True,
            "count": len(items),
            "items": [{"item_id": i.item_id, "name": i.item_name} for i in items],
        }
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


def get_system_overview() -> dict:
    try:
        player_count = session.query(Player).count()
        group_count = session.query(Group).count()
        return {
            "status": "operational",
            "total_players": player_count,
            "total_groups": group_count,
        }
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


def get_player_personal_bests(player_name: str) -> dict:
    try:
        player = _find_player(player_name)
        if not player:
            return {"found": False, "message": f"No player found matching '{player_name}'."}

        pbs = (
            session.query(PersonalBestEntry)
            .filter_by(player_id=player.player_id)
            .order_by(PersonalBestEntry.personal_best.asc())
            .limit(20)
            .all()
        )

        pb_list = []
        for pb in pbs:
            npc = (
                session.query(NpcList).filter_by(npc_id=pb.npc_id).first()
                if pb.npc_id
                else None
            )
            pb_list.append(
                {
                    "boss": npc.npc_name if npc else f"Boss #{pb.npc_id}",
                    "personal_best_ms": pb.personal_best,
                    "time": _ms_to_time_str(pb.personal_best) if pb.personal_best else "N/A",
                    "team_size": pb.team_size,
                }
            )

        return {
            "found": True,
            "player_name": player.player_name,
            "personal_bests": pb_list,
        }
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


def get_group_config(group_identifier: str) -> dict:
    try:
        group = _find_group(group_identifier)
        if not group:
            return {"found": False, "message": f"No group found for '{group_identifier}'."}

        configs = (
            session.query(GroupConfiguration)
            .filter_by(group_id=group.group_id)
            .all()
        )

        # Expose only safe, non-sensitive config fields
        safe_keys = {
            "min_value_to_notify",
            "min_value_to_post",
            "send_clan_notifications",
            "notify_on_pb",
            "notify_on_ca",
            "notify_on_clog",
            "lootboard_enabled",
            "wom_group_id",
        }

        config_data = {}
        for cfg in configs:
            key = getattr(cfg, "config_key", None) or getattr(cfg, "key", None)
            value = getattr(cfg, "config_value", None) or getattr(cfg, "value", None)
            if key and key in safe_keys:
                config_data[key] = value

        return {
            "found": True,
            "group_name": group.group_name,
            "config": config_data,
        }
    except Exception as exc:
        session.rollback()
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Tool dispatcher
# ---------------------------------------------------------------------------

_TOOL_MAP = {
    "lookup_player": lambda a: lookup_player(a["player_name"]),
    "lookup_group": lambda a: lookup_group(a["identifier"]),
    "get_player_recent_drops": lambda a: get_player_recent_drops(
        a["player_name"], a.get("limit", 5)
    ),
    "get_group_members": lambda a: get_group_members(a["group_identifier"]),
    "search_items": lambda a: search_items(a["item_name"]),
    "get_system_overview": lambda a: get_system_overview(),
    "get_player_personal_bests": lambda a: get_player_personal_bests(a["player_name"]),
    "get_group_config": lambda a: get_group_config(a["group_identifier"]),
}


def execute_tool(tool_name: str, tool_input: dict) -> Any:
    """Dispatch a tool call by name. All tools are read-only."""
    fn = _TOOL_MAP.get(tool_name)
    if fn is None:
        return {"error": f"Unknown tool: {tool_name}"}
    try:
        return fn(tool_input)
    except Exception as exc:
        return {"error": f"Tool '{tool_name}' failed: {exc}"}

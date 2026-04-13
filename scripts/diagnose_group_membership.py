"""
Diagnostic script: inspect group membership sync state for a specific player and WOM group.

Usage:
    python scripts/diagnose_group_membership.py [player_name] [wom_group_id]

Defaults: player_name="kerzington", wom_group_id=6711

The script:
  1. Looks up the player and group in the local DB.
  2. Calls the WOM API to fetch the group's full member list.
  3. Compares the DB view vs. WOM view and reports exactly what _sync_group_from_wom
     would do (dry-run — no writes are made).
"""

import sys
import asyncio
import os

# Ensure project root is on the path so relative imports work from the scripts/ dir.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from db.models.base import session as db_session
from db.models.player import Player
from db.models.group import Group
from db.models.associations import user_group_association
from sqlalchemy import text

import wom as wom_lib


# ── helpers ───────────────────────────────────────────────────────────────────

def _hr(title: str = ""):
    print("\n" + ("─" * 60) + (f"  {title}" if title else ""))


def _extract_group_metadata(details):
    """
    Support multiple wom.py response shapes.

    Some versions expose group metadata directly on the details object
    (`details.name`, `details.member_count`) while others nest it under
    `details.group`.
    """
    nested_group = getattr(details, "group", None)
    wom_name = getattr(nested_group, "name", None)
    member_count = getattr(nested_group, "member_count", None)

    if wom_name is None:
        wom_name = getattr(details, "name", None)
    if member_count is None:
        member_count = getattr(details, "member_count", None)

    try:
        member_count = int(member_count) if member_count is not None else None
    except (TypeError, ValueError):
        member_count = None

    return wom_name, member_count


def _check_db(player_name: str, wom_group_id: int):
    _hr("DB: player record")
    player = db_session.query(Player).filter(Player.player_name == player_name).first()
    if player is None:
        print(f"  [NOT FOUND] No player with name '{player_name}' in the DB.")
        return None, None
    print(f"  player_id : {player.player_id}")
    print(f"  player_name: {player.player_name}")
    print(f"  wom_id    : {player.wom_id}")
    if player.user:
        print(f"  user_id   : {player.user.user_id}  discord_id: {player.user.discord_id}")
    else:
        print("  user_id   : (no linked user)")

    _hr("DB: group record")
    group = db_session.query(Group).filter(Group.wom_id == wom_group_id).first()
    if group is None:
        print(f"  [NOT FOUND] No group with wom_id={wom_group_id} in the DB.")
        return player, None
    print(f"  group_id  : {group.group_id}")
    print(f"  group_name: {group.group_name}")
    print(f"  wom_id    : {group.wom_id}")

    _hr("DB: membership check")
    row = db_session.execute(
        text(
            "SELECT * FROM user_group_association "
            "WHERE group_id = :gid AND player_id = :pid"
        ),
        {"gid": group.group_id, "pid": player.player_id},
    ).fetchone()
    if row:
        print(f"  [PRESENT]  {player.player_name} IS in group {group.group_name} (association row: {dict(row._mapping)})")
    else:
        print(f"  [ABSENT]   {player.player_name} is NOT in group {group.group_name} in the DB.")

    _hr("DB: all players currently in group")
    group_players = list(group.players)
    print(f"  Total DB members: {len(group_players)}")
    for p in group_players:
        marker = " <-- TARGET" if p.player_name == player_name else ""
        print(f"    player_id={p.player_id:6}  wom_id={str(p.wom_id):10}  name={p.player_name}{marker}")

    return player, group


async def _check_wom(wom_group_id: int, player_wom_id: int | None, player_name: str = ""):
    WOM_API_KEY = os.getenv("WOM_API_KEY")
    client = wom_lib.Client(WOM_API_KEY, user_agent="@joelhalen-diagnostic")
    await client.start()
    try:
        _hr("WOM API: raw group details")
        result = await client.groups.get_details(wom_group_id)
        if not result.is_ok:
            print(f"  [ERROR] WOM API call failed: {result}")
            return None, None

        details = result.unwrap()
        wom_members = details.memberships
        wom_name, reported_count = _extract_group_metadata(details)

        print(f"  Group name     : {wom_name}")
        print(f"  member_count   : {reported_count}  (as reported by the API response)")
        print(f"  Memberships len: {len(wom_members)}  (actual items in memberships list)")

        if reported_count is not None and len(wom_members) < reported_count:
            print(
                f"\n  *** INCOMPLETE RESPONSE DETECTED ***\n"
                f"  WOM returned {len(wom_members)} member records but claims {reported_count} members.\n"
                f"  The skip_removals guard introduced in this fix would prevent erroneous removals."
            )
        elif reported_count is not None:
            print(f"\n  Response looks complete ({len(wom_members)} == {reported_count}).")

        wom_ids = {m.player_id for m in wom_members}

        _hr("WOM API: target player membership check")
        target_name = (player_name or "").lower()
        if player_wom_id is not None:
            if player_wom_id in wom_ids:
                print(f"  [PRESENT]  wom_id {player_wom_id} IS in the WOM member list.")
            else:
                print(f"  [ABSENT]   wom_id {player_wom_id} is NOT in the WOM member list.")
                for m in wom_members:
                    m_player = getattr(m, "player", None)
                    display_name = getattr(m_player, "display_name", None)
                    if target_name and display_name and target_name in display_name.lower():
                        print(
                            "  HINT: found a member whose name contains the target string: "
                            f"id={m.player_id}  name={display_name}"
                        )
        else:
            print("  Cannot check: player has no wom_id in the DB.")

        return wom_ids, wom_members
    finally:
        await client.close()


def _dry_run_sync(player: Player, group: Group, wom_ids: set, wom_members):
    """Simulate _sync_group_from_wom (read-only) and report what it would do."""
    _hr("DRY-RUN: what _sync_group_from_wom would do")

    db_player_wom_ids = {p.wom_id for p in group.players if p.wom_id}

    # All DB players in group by player_id for name lookup
    db_players_by_wom_id = {p.wom_id: p for p in group.players if p.wom_id}

    # WOM members that exist in our DB
    db_known_wom_ids = {
        p.wom_id
        for p in db_session.query(Player).filter(Player.wom_id.in_(wom_ids)).all()
    }

    to_remove = db_player_wom_ids - wom_ids
    to_add = db_known_wom_ids - db_player_wom_ids

    wom_members_by_id = {m.player_id: m for m in wom_members}

    print(f"\n  Would REMOVE {len(to_remove)} member(s):")
    for wid in sorted(to_remove):
        p = db_players_by_wom_id.get(wid)
        name = p.player_name if p else "?"
        marker = " <-- TARGET" if p and p.player_name == (player.player_name if player else "") else ""
        print(f"    wom_id={wid}  name={name}{marker}")

    print(f"\n  Would ADD {len(to_add)} member(s):")
    for wid in sorted(to_add):
        m = wom_members_by_id.get(wid)
        name = m.player.display_name if m and m.player else "?"
        print(f"    wom_id={wid}  name={name}")

    if player and player.wom_id in to_remove:
        print(
            f"\n  *** ROOT CAUSE CONFIRMED ***\n"
            f"  {player.player_name} (wom_id={player.wom_id}) would be REMOVED because their\n"
            f"  WOM ID is not present in the WOM member list for this group.\n"
            f"  Possible causes:\n"
            f"    1. WOM considers this player to have a different wom_id (name change created new identity).\n"
            f"    2. The player is genuinely no longer in the WOM group.\n"
            f"    3. The WOM response is incomplete (check member_count vs len above)."
        )
    elif player and player.wom_id not in db_player_wom_ids:
        print(
            f"\n  *** TARGET NOT CURRENTLY IN DB GROUP ***\n"
            f"  {player.player_name} is not in the DB group at all — "
            f"they would {'be added' if player.wom_id in to_add else 'NOT be added'} by the next sync."
        )
    else:
        print(
            f"\n  {player.player_name if player else '?'} would NOT be removed by the next sync."
        )


async def main():
    player_name = sys.argv[1] if len(sys.argv) > 1 else "kerzington"
    wom_group_id = int(sys.argv[2]) if len(sys.argv) > 2 else 6711

    print(f"Diagnosing membership for player='{player_name}', WOM group={wom_group_id}")

    player, group = _check_db(player_name, wom_group_id)
    player_wom_id = player.wom_id if player else None

    wom_ids, wom_members = await _check_wom(wom_group_id, player_wom_id, player_name)

    if group is not None and wom_ids is not None:
        _dry_run_sync(player, group, wom_ids, wom_members)

    _hr("Done")
    print()


if __name__ == "__main__":
    asyncio.run(main())

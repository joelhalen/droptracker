"""
Discord context-menu extension for modifying drop submissions.

Admins can right-click a drop notification message and choose "Modify Entry"
to delete, hide/unhide, edit the GP value, or change the split participants.
All changes propagate through the database, Redis cache, and point system.

After every edit the original notification embed is **rebuilt from the
group's template** so that all placeholder-driven fields (value, points,
ranks, split members …) stay accurate.  A "History" field at the bottom of
the embed accumulates a small-text audit trail of admin changes.
"""

import json
from datetime import datetime
from urllib.parse import quote

import interactions
from interactions import (
    ActionRow,
    Button,
    ButtonStyle,
    ComponentContext,
    ContextMenuContext,
    Embed,
    Extension,
    Message,
    Modal,
    Permissions,
    ShortText,
    message_context_menu,
    slash_default_member_permission,
)
from interactions.api.events import Component, ModalCompletion

from sqlalchemy import func as sa_func

from db.models import (
    Drop,
    Group,
    Guild,
    ItemList,
    NotifiedSubmission,
    NpcList,
    Player,
    PlayerPoints,
    Session,
    get_current_partition,
)
from services.redis_updates import RedisLootTracker, loot_tracker, get_player_list_loot_sum
from utils.redis import redis_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_session():
    return Session()


def _format_gp(value: int) -> str:
    return f"{value:,}"


def _embed_to_dict(embed) -> dict:
    """Safely serialise an interactions Embed to a plain dict."""
    try:
        return embed.to_dict()
    except Exception:
        d = {}
        if embed.title:
            d["title"] = embed.title
        if embed.description:
            d["description"] = embed.description
        if embed.color:
            d["color"] = embed.color.value if hasattr(embed.color, "value") else int(embed.color)
        if embed.url:
            d["url"] = embed.url
        if getattr(embed, "thumbnail", None):
            d["thumbnail"] = {"url": embed.thumbnail.url}
        if getattr(embed, "image", None):
            d["image"] = {"url": embed.image.url}
        if getattr(embed, "footer", None):
            d["footer"] = {"text": embed.footer.text}
            if getattr(embed.footer, "icon_url", None):
                d["footer"]["icon_url"] = embed.footer.icon_url
        if getattr(embed, "author", None):
            d["author"] = {"name": embed.author.name}
            if getattr(embed.author, "url", None):
                d["author"]["url"] = embed.author.url
            if getattr(embed.author, "icon_url", None):
                d["author"]["icon_url"] = embed.author.icon_url
        if embed.fields:
            d["fields"] = [
                {"name": f.name, "value": f.value, "inline": bool(f.inline)}
                for f in embed.fields
            ]
        return d


def _embed_from_dict(d: dict) -> Embed:
    """Reconstruct an Embed from a plain dict."""
    embed = Embed(
        title=d.get("title"),
        description=d.get("description"),
        color=d.get("color"),
        url=d.get("url"),
    )
    if d.get("thumbnail"):
        embed.set_thumbnail(url=d["thumbnail"]["url"])
    if d.get("image"):
        embed.set_image(url=d["image"]["url"])
    if d.get("footer"):
        embed.set_footer(
            text=d["footer"].get("text", ""),
            icon_url=d["footer"].get("icon_url"),
        )
    if d.get("author"):
        embed.set_author(
            name=d["author"].get("name", ""),
            url=d["author"].get("url"),
            icon_url=d["author"].get("icon_url"),
        )
    for field in d.get("fields", []):
        embed.add_field(name=field["name"], value=field["value"], inline=field.get("inline", False))
    return embed


# ---------------------------------------------------------------------------
# Embed rebuild — mirrors the notification service's template pipeline
# ---------------------------------------------------------------------------

async def _rebuild_notification_embed(drop, player, item, npc, group_id, db):
    """
    Rebuild the Discord notification embed from the group's template using
    current DB / Redis state.  Returns a fresh Embed or *None* on failure.
    """
    from db.ops import DatabaseOperations, get_formatted_name
    from db.xf.upgrades import check_active_upgrade
    from utils.format import replace_placeholders, format_number

    db_ops = DatabaseOperations()

    upgrade_active = check_active_upgrade(group_id)
    template_group = group_id if upgrade_active else 1
    embed_template = await db_ops.get_group_embed("drop", template_group)
    if not embed_template:
        return None

    player_id = player.player_id
    player_name = player.player_name
    item_name = item.item_name if item else "Unknown"
    item_id_val = item.item_id if item else 1
    npc_name = npc.npc_name if npc else "Unknown"
    npc_id_val = npc.npc_id if npc else 1
    quantity = int(drop.quantity or 1)
    total_value = int(drop.value or 0) * quantity
    kill_count = None

    # -- rank data from Redis --
    partition = get_current_partition()

    month_key = f"player:{player_id}:{partition}:total_loot"
    month_raw = redis_client.get(month_key)
    if month_raw is not None:
        month_total_int = int(month_raw)
    else:
        score = redis_client.client.zscore(f"leaderboard:{partition}", player_id)
        month_total_int = int(float(score)) if score is not None else 0
    player_month_total = format_number(month_total_int)

    players_in_group = (
        db.query(Player.player_id)
        .join(Player.groups)
        .filter(Group.group_id == group_id)
        .all()
    )
    group_month_total = format_number(
        get_player_list_loot_sum([p.player_id for p in players_in_group])
    )

    global_rank_data = loot_tracker.get_player_rank(player_id, None, partition)
    group_rank_data = loot_tracker.get_player_rank(player_id, group_id, partition)

    if group_rank_data:
        group_rank, user_count = group_rank_data
    else:
        group_rank = None
        user_count = redis_client.client.zcard(f"leaderboard:{partition}:group:{group_id}")
    if global_rank_data:
        global_rank, total_global_players = global_rank_data
    else:
        global_rank = None
        total_global_players = redis_client.client.zcard(f"leaderboard:{partition}")

    all_groups = db.query(Group.group_id).filter(Group.group_id != 2).all()
    total_groups = max(len(all_groups) - 1, 1)
    group_totals = []
    for g in all_groups:
        gt = redis_client.zsum(f"leaderboard:{partition}:group:{g.group_id}")
        group_totals.append({"id": g.group_id, "total": gt})
    sorted_groups = sorted(group_totals, key=lambda x: x["total"], reverse=True)
    group_to_group_rank = str(
        next((i for i, g in enumerate(sorted_groups) if g["id"] == group_id), 0) + 1
    )

    formatted_name = get_formatted_name(player_name, group_id, db)

    global_rank_str = (
        f"`{global_rank}`/`{total_global_players}`"
        if global_rank is not None
        else "`?`"
    )
    group_rank_str = (
        f"`{group_rank}`/`{user_count}`"
        if group_rank is not None
        else "`?`"
    )

    # -- group points data (re-queried after mutations) --
    all_points = (
        db.query(PlayerPoints)
        .filter(PlayerPoints.entry_id == drop.drop_id, PlayerPoints.group_id == group_id)
        .all()
    )
    total_pts = sum(p.amount for p in all_points)

    members_awarded = []
    for pp in all_points:
        pp_player = db.query(Player).filter(Player.player_id == pp.player_id).first()
        pp_total = (
            db.query(sa_func.sum(PlayerPoints.amount))
            .filter(PlayerPoints.player_id == pp.player_id, PlayerPoints.group_id == group_id)
            .scalar()
        ) or 0
        members_awarded.append({
            "player_name": pp_player.player_name if pp_player else "Unknown",
            "player_id": pp.player_id,
            "points_awarded": pp.amount,
            "current_points": pp_total,
        })

    members_text = ", ".join(
        f"{m['player_name']} (+{m['points_awarded']}, total {m['current_points']})"
        for m in members_awarded
    )
    suppress_members = (
        len(members_awarded) == 1
        and members_awarded[0].get("player_id") == player_id
    )

    receiver_total = (
        db.query(sa_func.sum(PlayerPoints.amount))
        .filter(PlayerPoints.player_id == player_id, PlayerPoints.group_id == group_id)
        .scalar()
    ) or 0

    video_url = getattr(drop, "video_url", None) or ""
    image_url = getattr(drop, "image_url", None) or ""

    values = {
        "{item_name}": item_name,
        "{month_name}": datetime.now().strftime("%B"),
        "{player_total_month}": f"`{player_month_total}`",
        "{global_rank}": global_rank_str,
        "{group_rank}": group_rank_str,
        "{user_count}": f"`{user_count}`",
        "{group_total_month}": f"`{group_month_total}`",
        "{group_to_group_rank}": f"`{group_to_group_rank}`/`{total_groups}`",
        "{item_id}": str(item_id_val),
        "{npc_id}": str(npc_id_val),
        "{npc_name}": npc_name,
        "{kill_count}": str(kill_count),
        "{item_value}": f"`{format_number(total_value)}`",
        "{quantity}": f"`{quantity}`",
        "{total_value}": f"`{total_value}`",
        "{player_name}": (
            f"[{player_name}](https://www.droptracker.io/players/"
            f"{quote(player_name, safe='')}.{player_id}/view)"
        ),
        "{image_url}": video_url or image_url,
        "{video_url}": video_url,
        "{video_link}": f"[Video]({video_url})" if video_url else "",
    }

    if total_pts > 0:
        values["{group_points_awarded}"] = str(total_pts)
    if members_awarded and members_text and not suppress_members:
        values["{group_points_member_count}"] = str(len(members_awarded))
        values["{group_points_members_awarded}"] = members_text
    if receiver_total > 0:
        values["{group_points_receiver_total}"] = str(receiver_total)

    embed = replace_placeholders(embed_template, values)

    # Strip unresolved group-point placeholders (mirrors _finalize_group_points_embed)
    _gp_phs = (
        "{group_points_awarded}",
        "{group_points_receiver_total}",
        "{group_points_member_count}",
        "{group_points_members_awarded}",
    )
    for attr in ("title", "description"):
        text = getattr(embed, attr, None)
        if text and any(p in text for p in _gp_phs):
            for p in _gp_phs:
                text = text.replace(p, "")
            setattr(embed, attr, text.strip() or None)
    if embed.footer and embed.footer.text:
        for p in _gp_phs:
            embed.footer.text = embed.footer.text.replace(p, "")
    if embed.fields:
        kept = []
        for f in embed.fields:
            combined = f"{f.name or ''} {f.value or ''}"
            if any(p in combined for p in _gp_phs):
                continue
            if str(f.value or "").strip() == "":
                continue
            kept.append(f)
        embed.fields = kept

    if group_id == 2 and embed.fields:
        embed.fields = [f for f in embed.fields if "Group" not in (f.name or "")]
    if embed.fields:
        embed.fields = [f for f in embed.fields if "Source:" not in (f.name or "")]

    return embed


def _extract_history(embed) -> str | None:
    """Extract the value of the History field from an embed, if present."""
    if not embed or not embed.fields:
        return None
    for f in embed.fields:
        if f.name == "History":
            return f.value
    return None


async def _rebuild_and_edit_message(bot, drop, player, item, npc, notified, db,
                                     history_line: str | None = None):
    """
    Rebuild the notification embed from template, carry forward the History
    field, optionally append a new history line, and edit the Discord message.
    """
    group_id = notified.group_id

    # Grab existing History field from the live message before we replace it
    old_embed = await _fetch_message_embed(bot, notified.channel_id, notified.message_id)
    existing_history = _extract_history(old_embed)

    new_embed = await _rebuild_notification_embed(drop, player, item, npc, group_id, db)
    if not new_embed:
        if history_line:
            await _update_history_field(
                bot, notified.channel_id, notified.message_id, history_line,
            )
        return

    # Re-attach the History field with the new line appended
    lines = existing_history or ""
    if history_line:
        new_line = f"-# {history_line}"
        lines = f"{lines}\n{new_line}" if lines else new_line
    if lines:
        new_embed.add_field(name="History", value=lines, inline=False)

    try:
        channel = await bot.fetch_channel(int(notified.channel_id))
        if channel:
            msg = await channel.fetch_message(int(notified.message_id))
            if msg:
                await msg.edit(embeds=[new_embed])
    except Exception as e:
        print(f"[EntryModifier] Could not edit message {notified.message_id}: {e}")


def _get_split_player_names(drop_id: int, group_id: int, receiver_player_id: int, db):
    """Return a list of player names that received split points for this drop."""
    rows = (
        db.query(PlayerPoints.player_id, Player.player_name)
        .join(Player, Player.player_id == PlayerPoints.player_id)
        .filter(
            PlayerPoints.entry_id == drop_id,
            PlayerPoints.group_id == group_id,
            PlayerPoints.player_id != receiver_player_id,
        )
        .all()
    )
    return [name for _, name in rows]


def _build_overview_embed(drop, item, npc, player, notified, split_names, all_points):
    """Build the ephemeral overview embed shown to the admin."""
    total_pts = sum(p.amount for p in all_points)
    status = "Hidden" if drop.hidden else "Active"

    desc_lines = [
        f"**Item:** {item.item_name} (x{drop.quantity})",
        f"**Player:** {player.player_name}",
        f"**NPC:** {npc.npc_name if npc else 'Unknown'}",
        f"**Value:** {_format_gp(drop.value * drop.quantity)} GP",
        f"**Date:** {drop.date_added.strftime('%b %d, %Y %H:%M') if drop.date_added else 'N/A'}",
        f"**Points Awarded:** {_format_gp(total_pts)}",
    ]
    if split_names:
        desc_lines.append(f"**Split Members:** {', '.join(split_names)}")
    desc_lines.append(f"**Status:** {status}")

    embed = Embed(
        title="Submission Details",
        description="\n".join(desc_lines),
        color=0x2B2D31,
    )
    embed.set_thumbnail(url=f"https://static.runelite.net/cache/item/icon/{drop.item_id}.png")
    embed.set_footer(text=f"Drop ID: {drop.drop_id} | Notified ID: {notified.id}")
    return embed


def _overview_buttons(notified_id: int, is_hidden: bool):
    hide_label = "Unhide" if is_hidden else "Hide"
    hide_style = ButtonStyle.SUCCESS if is_hidden else ButtonStyle.SECONDARY
    return [
        ActionRow(
            Button(label="Delete", style=ButtonStyle.DANGER, custom_id=f"entry_delete_{notified_id}"),
            Button(label=hide_label, style=hide_style, custom_id=f"entry_hide_{notified_id}"),
            Button(label="Edit Value", style=ButtonStyle.PRIMARY, custom_id=f"entry_edit_value_{notified_id}"),
            Button(label="Modify Splits", style=ButtonStyle.PRIMARY, custom_id=f"entry_modify_splits_{notified_id}"),
        )
    ]


def _load_context(notified_id: int, db):
    """Load all related objects for a NotifiedSubmission id.  Returns a dict or None."""
    notified = db.query(NotifiedSubmission).filter(NotifiedSubmission.id == notified_id).first()
    if not notified or not notified.drop_id:
        return None

    drop = db.query(Drop).filter(Drop.drop_id == notified.drop_id).first()
    if not drop:
        return None

    item = db.query(ItemList).filter(ItemList.item_id == drop.item_id).first()
    npc = db.query(NpcList).filter(NpcList.npc_id == drop.npc_id).first()
    player = db.query(Player).filter(Player.player_id == drop.player_id).first()

    all_points = (
        db.query(PlayerPoints)
        .filter(PlayerPoints.entry_id == drop.drop_id, PlayerPoints.group_id == notified.group_id)
        .all()
    )
    split_names = _get_split_player_names(drop.drop_id, notified.group_id, drop.player_id, db)

    return {
        "notified": notified,
        "drop": drop,
        "item": item,
        "npc": npc,
        "player": player,
        "all_points": all_points,
        "split_names": split_names,
    }


async def _send_overview(ctx, notified_id: int, db, *, edit: bool = False):
    """Send (or edit) the overview embed for a notified submission."""
    data = _load_context(notified_id, db)
    if not data:
        text = "Could not load submission data."
        if edit:
            await ctx.edit_origin(content=text, embeds=[], components=[])
        else:
            await ctx.send(content=text, ephemeral=True)
        return

    embed = _build_overview_embed(
        data["drop"], data["item"], data["npc"], data["player"],
        data["notified"], data["split_names"], data["all_points"],
    )
    components = _overview_buttons(notified_id, data["drop"].hidden)

    if edit:
        await ctx.edit_origin(embeds=[embed], components=components, content="")
    else:
        await ctx.send(embeds=[embed], components=components, ephemeral=True)


# ---------------------------------------------------------------------------
# Backend mutation helpers
# ---------------------------------------------------------------------------

def _delete_points_for_drop(drop_id: int, group_id: int, db):
    """Delete all PlayerPoints rows linked to this drop in this group."""
    db.query(PlayerPoints).filter(
        PlayerPoints.entry_id == drop_id,
        PlayerPoints.group_id == group_id,
    ).delete(synchronize_session="fetch")


def _delete_points_for_drop_all_groups(drop_id: int, db):
    """Delete all PlayerPoints rows linked to this drop across ALL groups."""
    db.query(PlayerPoints).filter(
        PlayerPoints.entry_id == drop_id,
    ).delete(synchronize_session="fetch")


async def _re_award_points(drop, group_id, players_included, db):
    """Re-run the point award pipeline for a drop after modification."""
    from data.submissions.point_awards import check_and_award_points

    value = int(drop.value) * int(drop.quantity)
    await check_and_award_points(
        "drop",
        group_id,
        drop.player_id,
        value,
        players_included=players_included,
        item_id=drop.item_id,
        npc_id=drop.npc_id,
        quantity=int(drop.quantity),
        entry_id=drop.drop_id,
        external_session=db,
    )


def _redis_force_update(player_id: int, db):
    tracker = RedisLootTracker()
    tracker.force_update_player(player_id, session_to_use=db)


async def _try_delete_discord_message(bot, channel_id: str, message_id: str):
    try:
        channel = await bot.fetch_channel(int(channel_id))
        if channel:
            msg = await channel.fetch_message(int(message_id))
            if msg:
                await msg.delete()
    except Exception as e:
        print(f"[EntryModifier] Could not delete Discord message {message_id}: {e}")


async def _try_replace_embed(bot, channel_id: str, message_id: str, new_embed: Embed):
    """Replace the embed(s) on a message entirely (used for hide)."""
    try:
        channel = await bot.fetch_channel(int(channel_id))
        if channel:
            msg = await channel.fetch_message(int(message_id))
            if msg:
                await msg.edit(embeds=[new_embed])
    except Exception as e:
        print(f"[EntryModifier] Could not replace embed on message {message_id}: {e}")


async def _try_restore_embed(bot, channel_id: str, message_id: str, embed: Embed):
    """Restore a previously-cached embed onto a message."""
    try:
        channel = await bot.fetch_channel(int(channel_id))
        if channel:
            msg = await channel.fetch_message(int(message_id))
            if msg:
                await msg.edit(embeds=[embed])
    except Exception as e:
        print(f"[EntryModifier] Could not restore embed on message {message_id}: {e}")


async def _fetch_message_embed(bot, channel_id: str, message_id: str) -> Embed | None:
    """Fetch a message and return its first embed, or None."""
    try:
        channel = await bot.fetch_channel(int(channel_id))
        if channel:
            msg = await channel.fetch_message(int(message_id))
            if msg and msg.embeds:
                return msg.embeds[0]
    except Exception as e:
        print(f"[EntryModifier] Could not fetch embed from message {message_id}: {e}")
    return None


async def _update_history_field(bot, channel_id: str, message_id: str, line: str):
    """
    Append a line to the "History" field on the message's first embed.

    If a "History" field already exists, the new line is appended.
    Otherwise a new "History" field is created.  Each entry uses Discord
    small-text markdown (-# …) for a compact audit trail.
    """
    try:
        channel = await bot.fetch_channel(int(channel_id))
        if not channel:
            return
        msg = await channel.fetch_message(int(message_id))
        if not msg or not msg.embeds:
            return

        embed = msg.embeds[0]
        new_line = f"-# {line}"

        history_idx = None
        if embed.fields:
            for idx, field in enumerate(embed.fields):
                if field.name == "History":
                    history_idx = idx
                    break

        if history_idx is not None:
            existing = embed.fields[history_idx].value
            embed.fields[history_idx].value = f"{existing}\n{new_line}"
        else:
            embed.add_field(name="History", value=new_line, inline=False)

        await msg.edit(embeds=[embed])
    except Exception as e:
        print(f"[EntryModifier] Could not update History field on message {message_id}: {e}")


# ---------------------------------------------------------------------------
# Extension
# ---------------------------------------------------------------------------

class EntryModifier(Extension):
    def __init__(self, bot: interactions.Client):
        self.bot = bot
        print("[EntryModifier] Extension loaded.")

    # ------------------------------------------------------------------
    # Context menu entry point
    # ------------------------------------------------------------------

    @message_context_menu(name="Modify Entry")
    @slash_default_member_permission(Permissions.ADMINISTRATOR)
    async def modify_entry(self, ctx: ContextMenuContext):
        message: Message = ctx.target
        db = _get_session()
        try:
            # Fetch the guild row once, before any permission or lookup checks.
            guild_row = db.query(Guild).filter(Guild.guild_id == str(ctx.guild_id)).first()

            if not guild_row:
                await ctx.send("This guild is not configured or recognized.", ephemeral=True)
                return

            # Use the group_id associated with this guild to search for the notification.
            notified = (
                db.query(NotifiedSubmission)
                .filter(
                    NotifiedSubmission.message_id == str(message.id),
                    NotifiedSubmission.group_id == guild_row.group_id,
                )
                .first()
            )

            if not notified:
                await ctx.send("This message is not a tracked drop notification.", ephemeral=True)
                return

            if not notified.drop_id:
                await ctx.send("Only drop submissions can be modified via this menu.", ephemeral=True)
                return

            print(f"[EntryModifier] CTX Guild ID: {ctx.guild_id} Guild row's group id: {guild_row.group_id}")

            # Permission: guild must map to the same group, unless group_id is 2 (super-admin mode).
            if guild_row.group_id != 2 and guild_row.group_id != notified.group_id:
                print(
                    f"[EntryModifier] No permissions - Guild row's group id: {guild_row.group_id} Notified group id: {notified.group_id}"
                )
                await ctx.send("You do not have permission to modify entries for this group.", ephemeral=True)
                return

            await _send_overview(ctx, notified.id, db)
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Component router
    # ------------------------------------------------------------------

    @interactions.listen(Component)
    async def on_component(self, event: Component):
        ctx = event.ctx
        cid = ctx.custom_id

        if cid.startswith("entry_delete_confirm_"):
            await self._handle_delete_confirm(ctx)
        elif cid.startswith("entry_delete_"):
            await self._handle_delete_prompt(ctx)
        elif cid.startswith("entry_hide_"):
            await self._handle_hide_toggle(ctx)
        elif cid.startswith("entry_edit_value_"):
            await self._handle_edit_value_modal(ctx)
        elif cid.startswith("entry_modify_splits_"):
            await self._handle_modify_splits_modal(ctx)
        elif cid.startswith("entry_cancel_"):
            await ctx.edit_origin(content="Action cancelled.", embeds=[], components=[])

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    async def _handle_delete_prompt(self, ctx: ComponentContext):
        notified_id = int(ctx.custom_id.split("_")[-1])
        embed = Embed(
            title="Confirm Deletion",
            description=(
                "This will **permanently** remove this drop, all associated points, "
                "and update leaderboards. **This cannot be undone.**"
            ),
            color=0xED4245,
        )
        components = [
            ActionRow(
                Button(label="Confirm Delete", style=ButtonStyle.DANGER, custom_id=f"entry_delete_confirm_{notified_id}"),
                Button(label="Cancel", style=ButtonStyle.SECONDARY, custom_id=f"entry_cancel_{notified_id}"),
            )
        ]
        await ctx.edit_origin(embeds=[embed], components=components, content="")

    async def _handle_delete_confirm(self, ctx: ComponentContext):
        notified_id = int(ctx.custom_id.split("_")[-1])
        db = _get_session()
        try:
            data = _load_context(notified_id, db)
            if not data:
                await ctx.edit_origin(content="Submission not found.", embeds=[], components=[])
                return

            drop = data["drop"]
            notified = data["notified"]
            player_id = drop.player_id
            # Capture before deletion invalidates the ORM object
            orig_channel_id = notified.channel_id
            orig_message_id = notified.message_id

            # 1. Delete points across all groups
            _delete_points_for_drop_all_groups(drop.drop_id, db)

            # 2. Delete all NotifiedSubmission records for this drop
            db.query(NotifiedSubmission).filter(NotifiedSubmission.drop_id == drop.drop_id).delete(synchronize_session="fetch")

            # 3. Delete the drop itself
            db.query(Drop).filter(Drop.drop_id == drop.drop_id).delete(synchronize_session="fetch")

            db.commit()

            # 4. Force update Redis
            _redis_force_update(player_id, db)

            # 5. Delete the original Discord message
            await _try_delete_discord_message(self.bot, orig_channel_id, orig_message_id)

            await ctx.edit_origin(
                content="Drop has been permanently deleted. Points reversed, leaderboards updated.",
                embeds=[],
                components=[],
            )
        except Exception as e:
            db.rollback()
            print(f"[EntryModifier] Delete failed: {e}")
            await ctx.edit_origin(content=f"Delete failed: {e}", embeds=[], components=[])
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Hide / Unhide
    # ------------------------------------------------------------------

    async def _handle_hide_toggle(self, ctx: ComponentContext):
        notified_id = int(ctx.custom_id.split("_")[-1])
        db = _get_session()
        try:
            data = _load_context(notified_id, db)
            if not data:
                await ctx.edit_origin(content="Submission not found.", embeds=[], components=[])
                return

            drop = data["drop"]
            notified = data["notified"]
            player_id = drop.player_id

            if not drop.hidden:
                # --- HIDE ---
                # Cache the current embed so we can restore it on unhide
                original_embed = await _fetch_message_embed(
                    self.bot, notified.channel_id, notified.message_id,
                )
                if original_embed:
                    notified.cached_embed = json.dumps(_embed_to_dict(original_embed))

                drop.hidden = True
                _delete_points_for_drop_all_groups(drop.drop_id, db)
                notified.status = "hidden"
                db.commit()

                _redis_force_update(player_id, db)

                hidden_embed = Embed(
                    title="[HIDDEN] Submission",
                    description="This submission has been hidden by an administrator.",
                    color=0x95A5A6,
                )
                await _try_replace_embed(self.bot, notified.channel_id, notified.message_id, hidden_embed)
            else:
                # --- UNHIDE ---
                # Recover the old History field from the cached embed
                old_history = None
                if notified.cached_embed:
                    try:
                        cached = _embed_from_dict(json.loads(notified.cached_embed))
                        old_history = _extract_history(cached)
                    except Exception:
                        pass

                drop.hidden = False
                notified.status = "sent"
                db.commit()

                # Re-award points for all groups this player is in
                from data.submissions.common import get_player_groups_with_global
                player = data["player"]
                player_groups = get_player_groups_with_global(db, player)
                for group in player_groups:
                    split_names = _get_split_player_names(drop.drop_id, group.group_id, drop.player_id, db)
                    await _re_award_points(drop, group.group_id, split_names or None, db)
                db.commit()

                _redis_force_update(player_id, db)

                # Rebuild the embed from template with updated data
                rebuilt = await _rebuild_notification_embed(
                    drop, player, data["item"], data["npc"], notified.group_id, db,
                )
                if rebuilt:
                    history_line = "Submission restored by an administrator"
                    lines = old_history or ""
                    new_line = f"-# {history_line}"
                    lines = f"{lines}\n{new_line}" if lines else new_line
                    rebuilt.add_field(name="History", value=lines, inline=False)
                    await _try_restore_embed(
                        self.bot, notified.channel_id, notified.message_id, rebuilt,
                    )
                else:
                    await _update_history_field(
                        self.bot, notified.channel_id, notified.message_id,
                        "Submission restored by an administrator",
                    )

                notified.cached_embed = None
                db.commit()

            # Re-render the overview for the admin
            await _send_overview(ctx, notified_id, db, edit=True)
        except Exception as e:
            db.rollback()
            print(f"[EntryModifier] Hide toggle failed: {e}")
            await ctx.edit_origin(content=f"Operation failed: {e}", embeds=[], components=[])
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Edit Value
    # ------------------------------------------------------------------

    async def _handle_edit_value_modal(self, ctx: ComponentContext):
        notified_id = int(ctx.custom_id.split("_")[-1])
        db = _get_session()
        try:
            data = _load_context(notified_id, db)
            if not data:
                await ctx.edit_origin(content="Submission not found.", embeds=[], components=[])
                return

            modal = Modal(
                ShortText(
                    label="New GP Value (per item)",
                    custom_id="new_value",
                    placeholder=str(data["drop"].value),
                    value=str(data["drop"].value),
                    required=True,
                ),
                title="Edit Drop Value",
                custom_id=f"entry_edit_value_modal_{notified_id}",
            )
            await ctx.send_modal(modal)
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Modify Splits
    # ------------------------------------------------------------------

    async def _handle_modify_splits_modal(self, ctx: ComponentContext):
        notified_id = int(ctx.custom_id.split("_")[-1])
        db = _get_session()
        try:
            data = _load_context(notified_id, db)
            if not data:
                await ctx.edit_origin(content="Submission not found.", embeds=[], components=[])
                return

            current_splits = ", ".join(data["split_names"]) if data["split_names"] else ""
            modal = Modal(
                ShortText(
                    label="Split players (comma-separated, or blank)",
                    custom_id="split_players",
                    placeholder="Player1, Player2, Player3",
                    value=current_splits,
                    required=False,
                ),
                title="Modify Split Players",
                custom_id=f"entry_modify_splits_modal_{notified_id}",
            )
            await ctx.send_modal(modal)
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Modal completions
    # ------------------------------------------------------------------

    @interactions.listen(ModalCompletion)
    async def on_modal(self, event: ModalCompletion):
        ctx = event.ctx
        cid = ctx.custom_id

        if cid.startswith("entry_edit_value_modal_"):
            await self._process_edit_value(ctx)
        elif cid.startswith("entry_modify_splits_modal_"):
            await self._process_modify_splits(ctx)

    async def _process_edit_value(self, ctx: ComponentContext):
        notified_id = int(ctx.custom_id.split("_")[-1])
        raw_value = ctx.responses.get("new_value", "").strip()

        # Accept values like "1,500,000,000" or "1500000000"
        cleaned = raw_value.replace(",", "").replace(" ", "")
        if not cleaned.isdigit():
            await ctx.send("Invalid value. Please enter a whole number.", ephemeral=True)
            return

        new_value = int(cleaned)
        db = _get_session()
        try:
            data = _load_context(notified_id, db)
            if not data:
                await ctx.send("Submission not found.", ephemeral=True)
                return

            drop = data["drop"]
            notified = data["notified"]

            # 1. Reverse existing points
            _delete_points_for_drop_all_groups(drop.drop_id, db)

            # 2. Update the drop value
            drop.value = new_value
            notified.edited_by = None  # We don't have user_id easily; could map later
            db.commit()

            # 3. Re-award points for all groups
            from data.submissions.common import get_player_groups_with_global
            player = data["player"]
            player_groups = get_player_groups_with_global(db, player)
            for group in player_groups:
                split_names = _get_split_player_names(drop.drop_id, group.group_id, drop.player_id, db)
                await _re_award_points(drop, group.group_id, split_names or None, db)
            db.commit()

            # 4. Force update Redis
            _redis_force_update(drop.player_id, db)

            # 5. Rebuild the notification embed so all fields reflect the change
            await _rebuild_and_edit_message(
                self.bot, drop, data["player"], data["item"], data["npc"], notified, db,
                history_line=f"Value updated to {_format_gp(new_value * drop.quantity)} GP",
            )

            # 6. Show updated overview
            await _send_overview(ctx, notified_id, db)
        except Exception as e:
            db.rollback()
            print(f"[EntryModifier] Edit value failed: {e}")
            await ctx.send(f"Edit failed: {e}", ephemeral=True)
        finally:
            db.close()

    async def _process_modify_splits(self, ctx: ComponentContext):
        notified_id = int(ctx.custom_id.split("_")[-1])
        raw_players = ctx.responses.get("split_players", "").strip()

        new_split_names = [p.strip() for p in raw_players.split(",") if p.strip()] if raw_players else []

        db = _get_session()
        try:
            data = _load_context(notified_id, db)
            if not data:
                await ctx.send("Submission not found.", ephemeral=True)
                return

            drop = data["drop"]
            notified = data["notified"]

            # 1. Reverse existing points for this group
            _delete_points_for_drop(drop.drop_id, notified.group_id, db)
            db.commit()

            # 2. Re-award with new split list
            await _re_award_points(drop, notified.group_id, new_split_names or None, db)
            notified.edited_by = None
            db.commit()

            # 3. Rebuild the notification embed so all fields reflect the change
            split_note = f"Split updated: {', '.join(new_split_names)}" if new_split_names else "Splits removed"
            await _rebuild_and_edit_message(
                self.bot, drop, data["player"], data["item"], data["npc"], notified, db,
                history_line=split_note,
            )

            # 4. Show updated overview
            await _send_overview(ctx, notified_id, db)
        except Exception as e:
            db.rollback()
            print(f"[EntryModifier] Modify splits failed: {e}")
            await ctx.send(f"Modify splits failed: {e}", ephemeral=True)
        finally:
            db.close()

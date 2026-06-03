"""
AISupportBot — discord-py-interactions Extension for the DropTracker AI support bot.

Responds to:
  • Messages in channels whose name contains a support keyword
  • Direct @-mentions of the bot in any channel
  • The /ask slash command (guild and DM-compatible)
  • The /support_clear slash command to reset conversation history
  • Button clicks from routing responses (ai_help_player, ai_help_group, ai_help_solo)

The knowledge base is checked here before falling through to Claude, so that
routing responses (which need to send Discord components) are handled at this
layer rather than inside the API client.
"""

import os
import time
from typing import Optional, Union

import interactions
from interactions import (
    ActionRow,
    Button,
    ButtonStyle,
    Extension,
    OptionType,
    SlashContext,
    listen,
    slash_command,
    slash_option,
)
from interactions.api.events import Component, MessageCreate, Startup

from .claude_client import ClaudeClient
from .knowledge_base import ROUTED_RESPONSES, match_knowledge

# Channel names (partial match) that trigger AI responses to plain messages
SUPPORT_CHANNEL_NAMES: frozenset[str] = frozenset(
    {
        "support",
        "ai-support",
        "help",
        "bot-support",
        "droptracker-support",
        "ask",
        "questions",
    }
)

RATE_LIMIT_SECONDS: float = float(os.getenv("AI_SUPPORT_RATE_LIMIT", "5"))
MIN_MESSAGE_LENGTH: int = 3


class AISupportBot(Extension):
    def __init__(self, bot: interactions.Client):
        self.bot = bot
        self.claude = ClaudeClient()
        # user_id -> monotonic timestamp of last query
        self._rate_limits: dict[int, float] = {}

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _cooldown_remaining(self, user_id: int) -> float:
        """Returns seconds until the user can query again, or 0.0 if ready."""
        last = self._rate_limits.get(user_id)
        if last is None:
            return 0.0
        elapsed = time.monotonic() - last
        remaining = RATE_LIMIT_SECONDS - elapsed
        return max(0.0, remaining)

    def _record_query(self, user_id: int) -> None:
        self._rate_limits[user_id] = time.monotonic()

    # ------------------------------------------------------------------
    # Knowledge base dispatch
    # ------------------------------------------------------------------

    def _build_route_components(self, buttons: list[dict]) -> list[ActionRow]:
        """Build an ActionRow from a routing dict's button list."""
        return [
            ActionRow(
                *[
                    Button(
                        label=b["label"],
                        style=ButtonStyle.PRIMARY,
                        custom_id=b["custom_id"],
                    )
                    for b in buttons
                ]
            )
        ]

    async def _handle_kb_response(
        self,
        kb_result: Union[str, dict],
        send_fn,
        ephemeral: bool = False,
    ) -> None:
        """
        Send a knowledge base response via *send_fn*.

        send_fn must accept keyword arguments: content, components, ephemeral.
        Plain str responses are sent as text; route dicts are sent with buttons.
        """
        if isinstance(kb_result, dict) and kb_result.get("type") == "route":
            components = self._build_route_components(kb_result["buttons"])
            await send_fn(
                content=kb_result["text"],
                components=components,
                ephemeral=ephemeral,
            )
        else:
            await send_fn(content=_truncate(str(kb_result)), ephemeral=ephemeral)

    # ------------------------------------------------------------------
    # Component handler (routing button clicks)
    # ------------------------------------------------------------------

    @listen(Component)
    async def on_component(self, event: Component) -> None:
        ctx = event.ctx
        custom_id = ctx.custom_id

        routed = ROUTED_RESPONSES.get(custom_id)
        if routed is None:
            return

        await ctx.send(content=_truncate(routed), ephemeral=True)

    # ------------------------------------------------------------------
    # Slash commands
    # ------------------------------------------------------------------

    @slash_command(
        name="ask",
        description="Ask the DropTracker AI support assistant a question",
    )
    @slash_option(
        name="question",
        description="Your question or support request",
        required=True,
        opt_type=OptionType.STRING,
    )
    async def ask_command(self, ctx: SlashContext, question: str) -> None:
        user_id = int(ctx.user.id)
        guild_id = int(ctx.guild_id) if ctx.guild_id else None

        cooldown = self._cooldown_remaining(user_id)
        if cooldown > 0:
            await ctx.send(
                f"Please wait **{cooldown:.1f}s** before asking another question.",
                ephemeral=True,
            )
            return

        self._record_query(user_id)

        # Check knowledge base before hitting Claude
        kb_result = match_knowledge(question)
        if kb_result is not None:
            await ctx.defer(ephemeral=False)

            async def _send(**kwargs):
                await ctx.send(**kwargs)

            await self._handle_kb_response(kb_result, _send)
            return

        # Fall through to Claude
        await ctx.defer()
        response = await self.claude.query(
            user_message=question,
            guild_id=guild_id,
            user_id=user_id,
            username=str(ctx.user.username),
        )
        await ctx.send(_truncate(response))

    @slash_command(
        name="support_clear",
        description="Clear your AI support conversation history",
    )
    async def clear_command(self, ctx: SlashContext) -> None:
        user_id = int(ctx.user.id)
        guild_id = int(ctx.guild_id) if ctx.guild_id else None
        self.claude.clear_history(guild_id, user_id)
        await ctx.send("Your conversation history has been cleared.", ephemeral=True)

    # ------------------------------------------------------------------
    # Message listener
    # ------------------------------------------------------------------

    @listen(MessageCreate)
    async def on_message(self, event: MessageCreate) -> None:
        message = event.message
        if not message or message.author.bot:
            return

        content: str = (message.content or "").strip()
        if not content:
            return

        bot_mention = f"<@{self.bot.user.id}>"
        bot_mention_nick = f"<@!{self.bot.user.id}>"

        mentioned = content.startswith(bot_mention) or content.startswith(bot_mention_nick)
        in_support_channel = _is_support_channel(message.channel)

        if not mentioned and not in_support_channel:
            return

        # Strip mention prefix if present
        for prefix in (bot_mention_nick, bot_mention):
            if content.startswith(prefix):
                content = content[len(prefix):].strip()
                break

        if len(content) < MIN_MESSAGE_LENGTH:
            return

        user_id = int(message.author.id)
        guild_id = int(message.guild.id) if message.guild else None

        cooldown = self._cooldown_remaining(user_id)
        if cooldown > 0:
            if mentioned:
                try:
                    await message.reply(
                        f"Please wait **{cooldown:.1f}s** before asking another question."
                    )
                except Exception:
                    pass
            return

        self._record_query(user_id)

        try:
            await message.channel.trigger_typing()
        except Exception:
            pass

        # Check knowledge base before hitting Claude
        kb_result = match_knowledge(content)
        if kb_result is not None:
            async def _send(**kwargs):
                # Message replies don't support ephemeral; drop that kwarg
                kwargs.pop("ephemeral", None)
                await message.reply(**kwargs)

            try:
                await self._handle_kb_response(kb_result, _send)
            except Exception as exc:
                print(f"[AISupportBot] Failed to send KB reply: {exc}")
            return

        # Fall through to Claude
        try:
            response = await self.claude.query(
                user_message=content,
                guild_id=guild_id,
                user_id=user_id,
                username=str(message.author.username),
            )
            await message.reply(_truncate(response))
        except Exception as exc:
            print(f"[AISupportBot] Failed to send Claude reply: {exc}")

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    @listen(Startup)
    async def on_startup(self, event: Startup) -> None:
        print("[AISupportBot] Extension ready.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_support_channel(channel) -> bool:
    if channel is None:
        return False
    name: str = getattr(channel, "name", "") or ""
    return any(kw in name.lower() for kw in SUPPORT_CHANNEL_NAMES)


def _truncate(text: str, limit: int = 2000) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 14] + "\n*(truncated)*"

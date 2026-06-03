"""
Anthropic Claude API client for the DropTracker AI support bot.

Manages per-user conversation history and runs the tool-use agentic loop.
Knowledge base matching is handled upstream in bot.py — this module only
deals with the Claude API.
"""

import asyncio
import json
import os
from collections import defaultdict
from typing import Optional

import anthropic

from .tools import TOOL_DEFINITIONS, execute_tool

SYSTEM_PROMPT = """You are DropTracker Support — a helpful AI assistant for the DropTracker OSRS drop-tracking application.

## What DropTracker Is
DropTracker is a system for Old School RuneScape (OSRS) players and clans that:
- Automatically tracks drops, personal bests, collection log entries, and combat achievements via a RuneLite plugin
- Sends Discord notifications for valuable or rare drops
- Maintains clan/group leaderboards and loot boards
- Integrates with Wise Old Man (WOM) for player verification and group member syncing
- Provides a web dashboard at https://www.droptracker.io

## Key Facts (use these — do not contradict them)
- There are NO API keys. Players just install the plugin and play; no registration step required.
- The command to link an OSRS account to Discord is /claim-rsn (not /link).
  The player must have received at least one drop with the plugin active before claiming.
- The command to create a new group is /create-group (requires a WOM group ID).
- Group configuration is done on the website: droptracker.io/account/players → group name → Configuration tab.
- WOM membership syncs automatically every ~2 hours. Admins can trigger a manual sync with /sync-wom (1-hour cooldown) or /force-group-sync to bypass the cooldown.
- Only the DropTracker RuneLite plugin is needed — no other plugins (Boss Timer, Collection Log, etc.) are required.
- Collection log tracking requires the in-game collection log popup notification to be enabled in OSRS game settings.
- Existing personal bests can be imported by visiting the Player-Owned House (POH) Achievement Gallery → Times tab while the plugin is active.
- Premium/upgrades page: droptracker.io/groups/upgrades
- Support Discord: droptracker.io/discord
- Wiki: droptracker.io/wiki
- RuneLite plugin install link: droptracker.io/runelite

## Your Role
- Answer questions about DropTracker features, setup, and troubleshooting
- Look up real data using the provided tools when users ask about specific players, groups, or items
- Guide users through setup, claiming their RSN, and configuring their clan

## Guidelines
- Use tools to retrieve actual data rather than guessing or fabricating stats
- If you can't find something in the database, say so clearly — don't invent it
- Keep responses concise; format for Discord (bold with **, bullet points with •, avoid markdown headers)
- If asked about something unrelated to OSRS or DropTracker, politely redirect
- Never reveal Discord IDs, tokens, or other private credentials
- If the user seems frustrated, be empathetic and offer to escalate to human support via droptracker.io/discord
- Refer users to https://www.droptracker.io for account management tasks you can't perform directly
"""

MAX_HISTORY = 20        # messages retained per user conversation
MAX_TOOL_ROUNDS = 5     # safety cap on tool-use iterations per query


class ClaudeClient:
    def __init__(self):
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is not set in the environment.")
        self.client = anthropic.AsyncAnthropic(api_key=api_key)
        self.model = os.getenv("AI_SUPPORT_MODEL", "claude-sonnet-4-6")
        # Keyed by "{guild_id}:{user_id}" — list of {"role": ..., "content": ...}
        self._history: dict[str, list] = defaultdict(list)

    # ------------------------------------------------------------------
    # Conversation history helpers
    # ------------------------------------------------------------------

    def _key(self, guild_id: Optional[int], user_id: int) -> str:
        return f"{guild_id or 'dm'}:{user_id}"

    def _append(self, key: str, role: str, content):
        self._history[key].append({"role": role, "content": content})
        # Trim oldest messages if over the limit (keep pairs to avoid orphaned tool results)
        history = self._history[key]
        while len(history) > MAX_HISTORY:
            history.pop(0)

    def clear_history(self, guild_id: Optional[int], user_id: int) -> None:
        self._history[self._key(guild_id, user_id)] = []

    # ------------------------------------------------------------------
    # Main query method
    # ------------------------------------------------------------------

    async def query(
        self,
        user_message: str,
        guild_id: Optional[int],
        user_id: int,
        username: str,
    ) -> str:
        key = self._key(guild_id, user_id)
        self._append(key, "user", user_message)

        # Build message list for this request (copy so we can extend locally
        # during tool-use rounds without permanently mutating history mid-loop)
        messages = list(self._history[key])

        for _round in range(MAX_TOOL_ROUNDS):
            try:
                response = await self.client.messages.create(
                    model=self.model,
                    max_tokens=1024,
                    system=SYSTEM_PROMPT,
                    messages=messages,
                    tools=TOOL_DEFINITIONS,
                )
            except anthropic.APIStatusError as exc:
                return f"⚠️ The AI service returned an error ({exc.status_code}). Please try again shortly."
            except anthropic.APIConnectionError:
                return "⚠️ Could not reach the AI service. Please try again in a moment."

            if response.stop_reason == "end_turn":
                # Extract the final text block
                text = next(
                    (block.text for block in response.content if hasattr(block, "text")),
                    "I wasn't able to generate a response. Please try rephrasing.",
                )
                # Persist assistant turn in history
                self._append(key, "assistant", response.content)
                return text

            if response.stop_reason == "tool_use":
                # Execute all tool calls (sync DB calls run in a thread pool)
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        result = await asyncio.to_thread(
                            execute_tool, block.name, block.input
                        )
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": json.dumps(result, default=str),
                            }
                        )

                # Append assistant + tool results to local message list and continue
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})
                continue

            # Unknown stop reason — exit loop
            break

        # Fallback if the tool loop exhausted without a final answer
        return "I was unable to complete your request. Please try again or rephrase your question."

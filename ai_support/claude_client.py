"""
Anthropic Claude API client for the DropTracker AI support bot.

Manages per-user conversation history, routes queries through the knowledge
base first, and runs the tool-use agentic loop when Claude needs live data.
"""

import asyncio
import json
import os
from collections import defaultdict
from typing import Optional

import anthropic

from .knowledge_base import match_knowledge
from .tools import TOOL_DEFINITIONS, execute_tool

SYSTEM_PROMPT = """You are DropTracker Support — a helpful AI assistant for the DropTracker OSRS drop-tracking application.

## What DropTracker Is
DropTracker is a system for Old School RuneScape (OSRS) players and clans that:
- Automatically tracks drops, personal bests, collection log entries, and combat achievements via a RuneLite plugin
- Sends Discord notifications for valuable or rare drops
- Maintains clan/group leaderboards and loot boards
- Integrates with Wise Old Man (WOM) for player verification and group syncing
- Provides a web dashboard at https://www.droptracker.io

## Your Role
- Answer questions about DropTracker features, setup, and troubleshooting
- Look up real data using the provided tools when users ask about specific players, groups, or items
- Guide users through setup, linking their RSN, and configuring their clan

## Guidelines
- Use tools to retrieve actual data rather than guessing or fabricating stats
- If you can't find something in the database, say so clearly — don't invent it
- Keep responses concise; format for Discord (bold with **, bullet points with •, no headers)
- If asked about something unrelated to OSRS or DropTracker, politely redirect
- Never reveal Discord IDs, API keys, tokens, or other private credentials
- If the user seems frustrated, be empathetic and offer to escalate to human support
- Refer users to https://www.droptracker.io for account management tasks you can't perform
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
        # Check the static knowledge base first — no API call needed
        kb_hit = match_knowledge(user_message)
        if kb_hit:
            return kb_hit

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

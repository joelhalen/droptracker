"""
DropTracker AI Support Bot — standalone entry point.

Runs a separate Discord bot that loads the ai_support Extension.
Configure via environment variables (see .env.example):
  AI_SUPPORT_BOT_TOKEN  — Discord bot token
  ANTHROPIC_API_KEY     — Anthropic API key for Claude
  AI_SUPPORT_MODEL      — Claude model to use (default: claude-sonnet-4-6)
  AI_SUPPORT_RATE_LIMIT — Per-user query cooldown in seconds (default: 5)
"""

import asyncio
import os
import random
import signal
import sys
import time

import interactions
from dotenv import load_dotenv

# Ensure the project root is on the path regardless of invocation directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

BOT_TOKEN = os.getenv("AI_SUPPORT_BOT_TOKEN")
if not BOT_TOKEN:
    print("ERROR: AI_SUPPORT_BOT_TOKEN is not set. Exiting.")
    sys.exit(1)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    print("ERROR: ANTHROPIC_API_KEY is not set. Exiting.")
    sys.exit(1)

RESTART_BASE_DELAY = 2
RESTART_MAX_DELAY = 60

bot = interactions.Client(
    token=BOT_TOKEN,
    intents=(
        interactions.Intents.DEFAULT
        | interactions.Intents.GUILD_MESSAGES
        | interactions.Intents.MESSAGE_CONTENT
        | interactions.Intents.DIRECT_MESSAGES
    ),
)

shutdown_event = asyncio.Event()


def _signal_handler(signum, frame):
    print(f"[AI Support Bot] Received signal {signum}, shutting down...")
    shutdown_event.set()


async def _stop_bot():
    try:
        if hasattr(bot, "stop"):
            result = bot.stop()
            if asyncio.iscoroutine(result):
                await result
        elif hasattr(bot, "close"):
            result = bot.close()
            if asyncio.iscoroutine(result):
                await result
    except Exception as exc:
        print(f"[AI Support Bot] Error stopping client: {exc}")


async def _run_with_restarts():
    attempt = 0
    while not shutdown_event.is_set():
        started_at = time.monotonic()
        try:
            print(f"[AI Support Bot] Starting (attempt {attempt + 1})...")
            await bot.astart(token=BOT_TOKEN)
            if shutdown_event.is_set():
                break
            print("[AI Support Bot] Client exited unexpectedly — scheduling restart.")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"[AI Support Bot] Crashed: {exc}")
        finally:
            await _stop_bot()

        uptime = time.monotonic() - started_at
        if uptime > 120:
            attempt = 0
        else:
            attempt += 1

        delay = min(RESTART_MAX_DELAY, RESTART_BASE_DELAY * (2 ** min(attempt, 5)))
        jitter = random.uniform(0.0, 1.0)
        wait = delay + jitter
        print(f"[AI Support Bot] Restarting in {wait:.1f}s...")
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=wait)
        except asyncio.TimeoutError:
            continue


async def main():
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # Load the AI support extension
    bot.load_extension("ai_support.bot")

    bot_task = asyncio.create_task(_run_with_restarts())
    shutdown_task = asyncio.create_task(shutdown_event.wait())

    done, pending = await asyncio.wait(
        [bot_task, shutdown_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    if not shutdown_event.is_set() and bot_task in done:
        exc = bot_task.exception()
        if exc:
            raise exc

    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await _stop_bot()
    print("[AI Support Bot] Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())

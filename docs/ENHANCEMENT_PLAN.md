# DropTracker — Enhancement Roadmap

**Created:** 2026-06-17  
**Branch:** `claude/enhancement-plan-2026`  
**Status:** Planning document — items are ordered by priority, not implementation order

---

## Context

This document was written after a full codebase audit. The core submission pipeline is stable and well-tested. The foundations laid so far (CLAUDE.md, ARCHITECTURE.md, SUBMISSION_PIPELINE.md, test suite) now enable confident large-scale changes. This plan focuses on what makes DropTracker feel polished and professional — to players, clan admins, and developers working on it.

**Already in-flight (open branches to be aware of):**
- `claude/ai-support-chatbot-discord-8QYO7` — AI support chatbot
- `claude/update-help-command-Z90k4` — help command improvements
- `claude/hall-of-fame-bugs-4MVQE` — Hall of Fame bug fixes
- `claude/wom-group-sync-feature-UKMtW` — WOM group sync feature

Do not duplicate work on these areas until those branches are merged.

---

## Priority Tier 1 — Foundation (enables everything else)

These items directly unblock later improvements and carry low regression risk.

---

### 1.1 Centralized `GroupConfig` Accessor

**Problem:** Group configuration reads (`group_configurations` K-V table) are scattered across every processor, the notification service, the lootboard generator, and slash commands. Each does its own raw SQLAlchemy query with no caching, inconsistent default values, and no central list of valid keys.

- `data/submissions/drop.py` — batch-queries configs inline
- `data/submissions/clog.py`, `pb.py`, `ca.py` — each queries independently
- `services/notification_service.py` — queries per-notification
- `utils/embeds.py` — direct session queries for `number_of_pbs_to_display`
- `commands/admin.py`, `commands/group_admin.py` — ad-hoc config writes

**Proposed solution:** Create `services/group_config.py` — a `GroupConfig` class with:
- A single read interface: `GroupConfig(session, group_id).get(key, default=...)`
- An in-process TTL cache (60s) keyed by `(group_id, config_key)`
- All valid config keys declared as class-level constants with their defaults
- A `set(key, value)` method for writes (validates against known keys)
- A `get_all()` method for bulk reads (single query, fills cache)

**Measurable outcome:** Every config read in the codebase uses `GroupConfig`. Zero raw `session.query(GroupConfiguration)` calls outside of `services/group_config.py`. New config keys are added in one place and automatically get defaults everywhere.

**Files to create/modify:**
- Create: `services/group_config.py`
- Modify: `data/submissions/drop.py`, `clog.py`, `pb.py`, `ca.py`, `pet.py`, `quest.py`
- Modify: `services/notification_service.py`
- Modify: `utils/embeds.py`

**Scope:** ~2 days

---

### 1.2 Structured API Response Format

**Problem:** `SubmissionResponse` is a plain Python class (`success`, `message`, `notice`). The API converts it to JSON inconsistently — callers can't distinguish error codes. Plugin developers debugging failed submissions have no machine-readable error classification.

**Current state in `api/routes/webhook.py`:**
```python
return jsonify({"success": response.success, "message": response.message})
```
This is missing: error codes, HTTP status alignment, and structured metadata.

**Proposed solution:**
- Add an `error_code` field to `SubmissionResponse` (e.g. `"DUPLICATE"`, `"AUTH_FAILED"`, `"ITEM_NOT_FOUND"`, `"NPC_NOT_FOUND"`, `"VALUE_UNVERIFIABLE"`)
- Return correct HTTP status codes: `200` for success, `409` for duplicate, `401` for auth failure, `422` for validation failures
- Add a `processing_time_ms` field to all responses (timing already tracked inside `drop_processor`)
- Standardize the JSON envelope across all submission types

**Measurable outcome:** The RuneLite plugin (and any future client) can handle errors programmatically. HTTP status codes are semantically correct. Response format is consistent and documented.

**Files to modify:**
- `data/submissions/common.py` — `SubmissionResponse` class
- `api/routes/webhook.py` — response building
- `data/submissions/drop.py`, all other processors — pass error codes on failure returns

**Scope:** ~1 day

---

### 1.3 Dead Code and Legacy Architecture Cleanup

**Problem:** Two things create confusion about the canonical code path:

1. `bots/main.py` contains an embedded Quart server (port 8080) that predates `api/app.py`. Both are started by `real_startup.sh`. The embedded server's routes overlap with the API's routes, but the API is the correct path. The embedded server has no clear active purpose.

2. `services/message_handler.py` contains the old webhook-channel submission parsing logic that was replaced by the API path. The code is commented out but still present, so the file looks important when it isn't.

**Proposed solution:**
- Audit `bots/main.py`'s embedded Quart routes — verify none are still in use — then remove the embedded `create_app()` call and the routes, leaving only the bot startup code
- In `services/message_handler.py`, delete the commented-out submission processing blocks, add a clear module docstring explaining that submission intake now goes through `api/routes/webhook.py`
- Update `real_startup.sh` to document which screen is which and why

**Measurable outcome:** `bots/main.py` is clearly a Discord bot entry point with no web server code. `services/message_handler.py` contains only active Discord event handler logic. Future developers are not confused about the canonical submission path.

**Files to modify:**
- `bots/main.py`
- `services/message_handler.py`
- `real_startup.sh`

**Scope:** ~half day

---

### 1.4 Expand Test Suite to Cover New Areas

**Problem:** The current test suite covers utilities and core submission helpers. The notification service (3,700 lines), API routes, and embed builders are completely untested.

**Priority additions:**
1. `tests/unit/test_group_config.py` — once `GroupConfig` is implemented (item 1.1), test it immediately
2. `tests/unit/test_notification_service.py` — test embed placeholder replacement, notification dedup logic, video URL resolution
3. `tests/integration/test_webhook_api.py` — activate the existing skeleton with proper Quart test client setup
4. `tests/unit/test_embeds.py` — test `sort_team_sizes()`, `embeds_are_equal()`, and `format_number` usage in embed builders

**Measurable outcome:** ≥150 total tests passing. Any refactor to `notification_service.py` or the group config system has test coverage before the change ships.

**Scope:** ~2 days (depends on item 1.1 being done first for GroupConfig tests)

---

## Priority Tier 2 — Player & Clan Admin UX

These make the product feel more polished for end users. No schema changes required.

---

### 2.1 Player Submission Failure Feedback

**Problem:** When a player's drop submission fails (auth mismatch, item not found, NPC not found, value unverifiable), they receive no feedback. The plugin shows a spinning animation, then nothing. The player doesn't know if the drop was tracked or not.

There's an existing `NotificationQueue` and DM infrastructure that handles `new_player`, `name_change`, etc. — the pattern is already there.

**Proposed solution:**
- Add a `"submission_failed"` notification type
- When a submission fails with a specific error code, create a `"submission_failed"` notification to the player's DM (only if `dm_drops` is enabled, to avoid spam)
- The DM includes: what failed (auth/item/NPC), the item name and value so the player knows which drop it was, and a support link
- For auth failures specifically, add a more prominent path for the player to re-authenticate (link to the web panel or a `/reauth` slash command)

**Measurable outcome:** Players have visibility into failed submissions. Support requests for "my drop wasn't tracked" are reduced.

**Files to modify:**
- `data/submissions/common.py` — add `"submission_failed"` notification creation
- `data/submissions/drop.py` — call it on each failure path
- `services/notification_service.py` — handle `"submission_failed"` type
- `utils/embeds.py` — add `create_submission_failed_embed()`

**Scope:** ~1 day

---

### 2.2 In-Discord Group Configuration (`/configure` command)

**Problem:** Group admins must use the web panel or raw DB tools to change group configuration (min value to notify, screenshot requirement, split GP tracking, etc.). There is no in-Discord way to change settings. Onboarding is difficult.

**Proposed solution:** Add a `/configure` slash command (admin-only) with:
- A selector for the config key to change (autocomplete from the known keys list)
- Current value shown in the response before the change
- Confirmation of the new value after setting
- A `/show-config` subcommand that lists all current non-default config values for the server

This depends on the `GroupConfig` accessor (item 1.1) so keys and defaults are in one place.

**Example interaction:**
```
/configure setting:minimum_value_to_notify value:1000000
→ "Changed 'Minimum value to notify' from 2,500,000 gp to 1,000,000 gp"
```

**Measurable outcome:** Clan admins can fully configure their group without leaving Discord or touching a web panel. Onboarding a new group requires only Discord commands.

**Files to modify/create:**
- `commands/admin.py` — add `/configure` and `/show-config` commands
- `services/group_config.py` — `set()` method (from item 1.1)
- `commands/utils.py` — add `is_group_admin()` check

**Scope:** ~1.5 days

---

### 2.3 Richer Drop Notification Embeds (Default Templates)

**Problem:** The default `GroupEmbed` templates for new groups are basic. Groups that haven't customized their embeds get a generic notification that doesn't surface the most useful context. The `{next_refresh}` placeholder and group rank data are available but not always shown.

From `utils/embeds.py`, the `get_global_drop_embed()` function already builds a rich embed with:
- Monthly total
- Global rank
- Group rank
- Player avatar

But the group-specific notification path (through `GroupEmbed` templates) may not include this data by default.

**Proposed solution:**
- Audit the default `GroupEmbed` rows inserted when a group is created
- Design improved default templates that include: player monthly total, global rank, group rank (when the drop was above the threshold), and a properly formatted value with `format_number()`
- Add a `{player_monthly_total}` and `{player_global_rank}` placeholder to the notification data dict
- Populate these in `notification_service.py` before placeholder replacement

**Measurable outcome:** Out-of-the-box drop notifications show the player's monthly total and rank. New groups look polished immediately without template customization.

**Files to modify:**
- `services/notification_service.py` — add rank/total data to the notification data dict
- `db/group_creation.py` — improve default GroupEmbed templates
- `utils/format.py` — no changes needed, already has `format_number`

**Scope:** ~1 day

---

### 2.4 `/stats` Player Summary Command

**Problem:** There is no in-Discord slash command that shows a player's current stats at a glance (monthly total, rank, top drops, collection log progress). The data exists in Redis and the DB — it's just not surfaced through Discord.

**Proposed solution:** Add a `/stats` command (available to all users) that shows:
- The player's monthly loot total (from Redis)
- Their global rank and group rank
- Their top 3 drops this month (from DB, limit 3)
- Collection log slot count
- All formatted into a Discord embed with the player's WOM avatar

Optional: `/stats player:<rsn>` for looking up other players.

**Measurable outcome:** Players can check their stats without visiting the website. Engagement with the bot increases.

**Files to modify:**
- `commands/user.py` — add `/stats` command
- `utils/embeds.py` — add `create_player_stats_embed()`
- `utils/redis.py` — no changes needed (data already in Redis)

**Scope:** ~1.5 days

---

### 2.5 Lootboard Theme Selector via Discord

**Problem:** Group admins cannot change their lootboard visual theme from Discord. The `loot_board_type` config key exists but requires web panel access or direct DB editing.

**Proposed solution:** Add a `/lootboard-theme` admin command with a visual selector (select menu showing available themes). After selection, update the `loot_board_type` group config and trigger an immediate lootboard regeneration for the group.

**Measurable outcome:** Admins can preview and switch themes from Discord in seconds.

**Files to modify:**
- `commands/admin.py` — add `/lootboard-theme`
- `services/group_config.py` — `set()` for `loot_board_type`
- `services/components.py` — add theme selector component

**Scope:** ~1 day

---

## Priority Tier 3 — Code Quality & Reliability

These items reduce maintenance burden and prevent future bugs.

---

### 3.1 Break Up `notification_service.py`

**Problem:** `notification_service.py` is 3,700 lines handling: notification dispatch, embed building per notification type (drop, pb, clog, ca, pet, quest), video attachment handling, XenForo alert creation, PB embed updates, video file management, and the notification polling loop. This is too much for one file.

**Proposed refactor:**
```
services/
  notification_service.py     # Polling loop + dispatch only (~300 lines)
  notification_builders/
    __init__.py               # Re-exports all builders
    drop_builder.py           # Build drop notification embed
    pb_builder.py             # Build PB notification embed
    clog_builder.py           # Build collection log embed
    ca_builder.py             # Build combat achievement embed
    pet_builder.py            # Build pet embed
    quest_builder.py          # Build quest embed
    dm_builder.py             # Build DM variants
```

Each builder takes `(notification_data: dict, group: Group, player: Player)` and returns a `(discord.Embed, [File])` tuple. The main service just dispatches to the right builder and sends the result.

**Measurable outcome:** Each notification type is testable in isolation. Adding a new notification type requires creating one new file, not editing a 3,700-line monolith. `notification_service.py` is under 400 lines.

**Scope:** ~3 days (high value, non-trivial refactor — do after test coverage is expanded)

---

### 3.2 Graceful Degradation Without XenForo

**Problem:** Several features silently fail or log errors when XenForo is not configured (`XF_KEY` absent, XF DB unreachable): group creation, premium upgrade checks, forum alerts. This affects development environments and deployments that don't use XenForo.

The issue is that `check_active_upgrade()` and `get_user_id()` are called without guards in places like `notification_service.py` and `commands/user.py`.

**Proposed solution:**
- Add `XF_ENABLED = bool(os.getenv("XF_KEY"))` to a config constants module
- Gate all XenForo calls behind `if XF_ENABLED:`
- For premium feature checks (`check_active_upgrade`): return `False` when XF is disabled rather than raising
- Document in `.env.example` which features require XenForo

**Measurable outcome:** The bot runs cleanly in dev environments without a XenForo instance. XF-dependent features degrade gracefully rather than logging exceptions.

**Files to modify:**
- Create: `utils/config.py` — app-wide constants derived from `.env`
- `services/notification_service.py` — gate XF calls
- `data/submissions/common.py` — gate `check_group_point_system_active` (uses raw XF SQL)
- `db/xf/upgrades.py` — return `False` guard at top when XF not configured

**Scope:** ~1 day

---

### 3.3 Structured Logging Improvements

**Problem:** Logging is inconsistent — some areas use `print()`, others use `AppLogger`, others use `logger.log_sync()`. There's no log level filtering in the console output. Under normal operation, `[DropPerf]` timing lines and `debug_print()` statements are mixed into the same output stream as actual errors.

**Proposed solution:**
- Replace all `print()` calls in processor files with structured `AppLogger.log()` calls
- Add a `LOG_LEVEL` env var (`debug`/`info`/`warning`/`error`) that gates `debug_print()` output
- Ensure all error paths include the player name, item name, and error type in the log entry so log analysis is possible without joining DB tables
- Remove commented-out `debug_print` blocks that add noise

**Measurable outcome:** `print()` is absent from all processor and service files. Log output is filterable by level. Error events include enough context to diagnose without a debugger.

**Files to modify:**
- `data/submissions/drop.py`, `common.py` — replace `print()` with structured logging
- `data/submissions/*.py` — same
- `services/notification_service.py` — replace print statements
- `db/app_logger.py` — add level filtering

**Scope:** ~1.5 days

---

## Priority Tier 4 — Infrastructure

These items improve CI/CD and the development feedback loop.

---

### 4.1 GitHub Actions CI Pipeline

**Problem:** There is no automated test runner. Tests exist but are only run manually. PRs can be merged without verification that the test suite passes.

**Proposed solution:** Add `.github/workflows/ci.yml` that:
1. Triggers on push to `new-api` and on any PR targeting `new-api`
2. Installs `requirements-dev.txt`
3. Runs `python -m pytest tests/unit/ -v --tb=short`
4. Fails the check if any unit test fails

This does not require a database or Redis — the unit test suite is fully stubbed.

**Measurable outcome:** Every PR has automated test verification. Broken tests block merge. Future agents and contributors see test results directly in the PR.

**Files to create:**
- `.github/workflows/ci.yml`

**Scope:** ~2 hours

---

### 4.2 Development Environment Script

**Problem:** There is no single command to start a development environment. The `real_startup.sh` is production-only (uses GNU screen). A new developer needs to read multiple files to understand what to run and in what order.

**Proposed solution:** Add `dev.sh`:
```bash
#!/bin/bash
# Starts the API and bot in foreground with dev settings
# Requires: .env with STATE=dev, Redis running locally
export STATE=dev
python -m pytest tests/unit/ -v --tb=short -q && \
  python api/app.py &
  python bots/main.py
```

Also add a `Makefile` with common targets:
```
make test       # run tests/unit/
make test-all   # run all tests including integration
make api        # start API only
make bot        # start bot only
make lint       # run any linter
```

**Measurable outcome:** `make test` is the first command any new contributor runs. The development loop is documented and reproducible.

**Files to create:**
- `dev.sh`
- `Makefile`

**Scope:** ~2 hours

---

## Summary Table

| # | Item | Tier | Scope | Unlocks |
|---|---|---|---|---|
| 1.1 | GroupConfig accessor | Foundation | 2 days | 1.4, 2.2, 2.5 |
| 1.2 | Structured API responses | Foundation | 1 day | 2.1 |
| 1.3 | Dead code cleanup | Foundation | 0.5 day | — |
| 1.4 | Expand test suite | Foundation | 2 days | 3.1 |
| 2.1 | Submission failure feedback | Player UX | 1 day | — |
| 2.2 | `/configure` command | Admin UX | 1.5 days | 2.5 |
| 2.3 | Richer default embeds | Player UX | 1 day | — |
| 2.4 | `/stats` command | Player UX | 1.5 days | — |
| 2.5 | Lootboard theme selector | Admin UX | 1 day | — |
| 3.1 | Break up notification_service | Code quality | 3 days | — |
| 3.2 | Graceful XenForo degradation | Reliability | 1 day | — |
| 3.3 | Structured logging | Code quality | 1.5 days | — |
| 4.1 | GitHub Actions CI | Infrastructure | 2 hours | — |
| 4.2 | Dev environment script | Infrastructure | 2 hours | — |

**Recommended order for maximum compounding value:**
1. `4.1` (CI) — first, so everything that follows has automated verification
2. `1.3` (dead code) — fast, clears confusion
3. `1.1` (GroupConfig) — enables 1.4, 2.2, 2.5
4. `1.4` (expand tests) — before touching notification_service
5. `1.2` (API responses) + `2.1` (failure feedback) — together, one PR
6. `2.2` (configure command) + `2.4` (stats command) — together, one PR
7. `2.3` (default embeds) + `2.5` (theme selector) — together, one PR
8. `3.1` (notification_service split) — after test coverage is in place
9. `3.2` + `3.3` — cleanup, can be done any time

---

## Non-Goals (Explicitly Out of Scope for This Roadmap)

- **Async queue refactor** (REFACTOR_PLAN.md) — the webhook latency issue is not currently impactful enough to justify the risk
- **New submission types** — the current 8 types cover OSRS well
- **Lootboard visual redesign** — the existing themes are adequate; improvements should be incremental
- **Multi-game support** — DropTracker is OSRS-specific by design
- **Public REST API** (external consumers) — not enough demand to justify versioning overhead now

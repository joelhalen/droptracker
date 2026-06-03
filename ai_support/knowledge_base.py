"""
Static knowledge base for DropTracker support.

Entries are matched against user queries using keyword scoring.
These are checked before hitting the Claude API to provide instant
answers to common questions without consuming API tokens.

match_knowledge() returns one of:
  - str        plain text response
  - dict       component routing response {"type": "route", "text": ..., "buttons": [...]}
  - None       no match — caller should fall through to Claude
"""

from typing import Union, Optional

# ---------------------------------------------------------------------------
# Routed responses — returned when a routing button is clicked
# ---------------------------------------------------------------------------

ROUTED_RESPONSES: dict[str, str] = {
    "ai_help_player": (
        "**Player Setup Guide**\n\n"
        "Getting started is quick — here's everything you need:\n\n"
        "**Step 1 — Install the DropTracker plugin**\n"
        "Search for **\"DropTracker\"** in the RuneLite Plugin Hub and enable it. "
        "Open the plugin's settings panel to configure which achievements you want tracked.\n\n"
        "**Step 2 — Play the game**\n"
        "You must receive at least one drop (or other tracked achievement) with the plugin active "
        "before claiming your account. This creates your player record in our database.\n\n"
        "**Step 3 — Claim your RSN**\n"
        "Use `/claim-rsn` in your clan's Discord server (or here), entering your character name "
        "**exactly as it appears in-game**. This links your Discord account to your OSRS character "
        "so you get pinged when your drops are posted.\n\n"
        "> Enabling **API Connections** in the plugin settings gives you the most accurate tracking — "
        "[find out why](https://www.droptracker.io/wiki/why-api).\n\n"
        "**Other useful commands:**\n"
        "• `/accounts` — View your currently claimed accounts\n"
        "• `/unclaim-rsn` — Remove an account from your Discord\n"
        "• `/pingme` — Toggle drop notification pings\n"
        "• `/hideme` — Hide yourself from global leaderboards\n"
        "• `/dm-settings` — Configure bot DM notifications\n\n"
        "Still have questions? Just ask!"
    ),

    "ai_help_group": (
        "**Group/Clan Setup Guide**\n\n"
        "**Before you begin — prerequisites:**\n"
        "1. A [WiseOldMan group](https://wiseoldman.net/groups) — "
        "[create one here](https://wiseoldman.net/groups/create) if you don't have one. "
        "Your **WOM Group ID** (3–6 digits) is found in your group's page URL.\n"
        "2. A Discord server where you have administrator permissions.\n"
        "3. The DropTracker bot "
        "[invited to your server](https://discord.com/oauth2/authorize?client_id=1172933457010245762&permissions=8&scope=bot).\n\n"
        "**Setup steps:**\n"
        "1. Run `/create-group` in your Discord server and enter your WOM Group ID when prompted.\n"
        "2. You'll receive a welcome DM with a link to your config page. You can also go there "
        "directly: **[droptracker.io/account/players](https://www.droptracker.io/account/players)** "
        "→ click your group name in the side navigation → open the **Configuration** tab.\n"
        "3. Set your **notification channel**, **lootboard channel**, and **minimum drop value** threshold.\n\n"
        "> The bot needs **Send Messages, Embed Links, and Attach Files** in the channels you configure "
        "— or grant it Administrator.\n\n"
        "**Useful admin commands:**\n"
        "• `/sync-wom` — Sync WOM group membership (1-hour cooldown)\n"
        "• `/force-group-sync` — Force a full sync, bypassing the cooldown\n"
        "• `/send_player_faq` — Post a player setup guide to a channel in your server\n"
        "• `/reset-group-points` — Reset all group points to zero\n\n"
        "Your members need to install the DropTracker plugin and use `/claim-rsn` to associate "
        "their accounts. Drops are tracked from the date each member installed the plugin.\n\n"
        "For premium features: [droptracker.io/groups/upgrades](https://www.droptracker.io/groups/upgrades)"
    ),

    "ai_help_solo": (
        "**Solo Player Setup Guide**\n\n"
        "DropTracker works great without a clan! Here's how to get started:\n\n"
        "**Step 1 — Install the DropTracker plugin**\n"
        "Search for **\"DropTracker\"** in the RuneLite Plugin Hub and enable it. "
        "Open the plugin settings to choose which achievements you want tracked "
        "(drops, PBs, collection log, combat achievements).\n\n"
        "**Step 2 — Play the game**\n"
        "Tracking begins automatically once you receive loot or achievements with the plugin active.\n\n"
        "**Step 3 — (Optional) Claim your RSN**\n"
        "Use `/claim-rsn` here to link your Discord to your OSRS account. "
        "This lets you get pinged when your drops appear in DropTracker's global channels.\n\n"
        "> Enable **API Connections** in the plugin settings for the best tracking accuracy — "
        "[learn why](https://www.droptracker.io/wiki/why-api).\n"
        "> Make sure the **in-game collection log popup** is turned on in your OSRS game settings, "
        "or collection log slots won't track correctly.\n\n"
        "**Your stats are always available at [droptracker.io](https://www.droptracker.io)** — "
        "view your drop history, personal bests, collection log progress, and combat achievements "
        "from your player profile.\n\n"
        "If you later join a clan that uses DropTracker, your historical drops will be counted "
        "from the date you installed the plugin."
    ),
}

# ---------------------------------------------------------------------------
# Keyword-matched knowledge base
# ---------------------------------------------------------------------------

KnowledgeResponse = Union[str, dict]

KNOWLEDGE_BASE: list[dict] = [
    {
        "keywords": [
            "how to set up", "how do i set up", "setup", "getting started",
            "get started", "install plugin", "runelite plugin", "how to install",
            "how do i start", "where do i begin", "first time", "new here",
        ],
        # Returns a routing dict — bot.py renders this as buttons
        "response": {
            "type": "route",
            "text": (
                "**Welcome to DropTracker Support!**\n\n"
                "To point you in the right direction — which of these best describes your situation?"
            ),
            "buttons": [
                {"label": "I'm a player / clan member",  "custom_id": "ai_help_player"},
                {"label": "I'm setting up a new group",  "custom_id": "ai_help_group"},
                {"label": "I'm a solo player (no clan)", "custom_id": "ai_help_solo"},
            ],
        },
    },
    {
        "keywords": [
            "claim rsn", "link account", "register username",
            "link my account", "link rsn", "how to claim", "connect account",
            "claim my account", "associate account", "claim-rsn",
        ],
        "response": (
            "**Claiming Your RSN (RuneScape Name)**\n\n"
            "1. **Install the DropTracker plugin** on RuneLite (Plugin Hub → search \"DropTracker\").\n"
            "2. **Receive at least one drop** (or other achievement) in-game with the plugin enabled — "
            "this creates your record in our database.\n"
            "3. **Use `/claim-rsn`** in a Discord server with the DropTracker bot (or here), "
            "entering your character name **exactly as it appears in-game**.\n"
            "4. The bot will confirm once your account is successfully linked.\n\n"
            "**Having trouble?**\n"
            "If your account doesn't appear, visit [wiseoldman.net](https://wiseoldman.net), "
            "search your username, and press **Update** to refresh it — then try `/claim-rsn` again.\n\n"
            "You can view your linked accounts at any time with `/accounts`, "
            "and remove one with `/unclaim-rsn`."
        ),
    },
    {
        "keywords": [
            "not tracking", "drops not showing", "missing drops", "drops not working",
            "not recording", "drops disappearing", "plugin not working", "not submitting",
            "drops missing", "nothing tracking", "loot not tracking",
        ],
        "response": (
            "**Drops Not Tracking? Troubleshooting Steps:**\n\n"
            "• **Plugin enabled?** Check the RuneLite sidebar — ensure the DropTracker plugin is active.\n"
            "• **API Connections enabled?** Open the plugin config and confirm **API Connections** is turned on. "
            "This is the most reliable submission method — [learn why it matters](https://www.droptracker.io/wiki/why-api).\n"
            "• **Submissions disabled?** Double-check that the plugin config doesn't have drop submissions toggled off.\n"
            "• **Account not appearing in your group?** Visit "
            "[droptracker.io/account/players](https://www.droptracker.io/account/players), "
            "log in with Discord, and confirm you appear in your group's member list.\n"
            "• **Specific NPC or item missing?** Let me know the name exactly as it appears in-game "
            "— I can check whether it exists in our database.\n\n"
            "Still stuck? Describe what's happening and I'll dig deeper."
        ),
    },
    {
        "keywords": [
            "what is droptracker", "about droptracker", "what does droptracker do",
            "explain droptracker", "how does droptracker work",
        ],
        "response": (
            "**About DropTracker**\n\n"
            "DropTracker is an OSRS (Old School RuneScape) drop and achievement tracking ecosystem:\n\n"
            "• **RuneLite Plugin** — Automatically submits drops, personal bests, collection log entries, "
            "and combat achievements as you play.\n"
            "• **Discord Bot** — Posts drop notifications to clan channels and generates loot leaderboards.\n"
            "• **Web Dashboard** — View detailed stats, leaderboards, and history at "
            "[droptracker.io](https://www.droptracker.io).\n"
            "• **Group/Clan Support** — Clan-wide leaderboards, configurable notification thresholds, "
            "and WiseOldMan group syncing.\n"
            "• **Wise Old Man Integration** — Player verification and automatic group member management.\n\n"
            "It's designed for OSRS clans who want full visibility into their members' loot and achievements."
        ),
    },
    {
        "keywords": [
            "loot board", "lootboard", "leaderboard setup", "notification channel",
            "set up notifications", "drop notifications", "configure notifications",
            "configure bot", "bot configuration", "configure channels",
        ],
        "response": (
            "**Setting Up Loot Boards & Notifications**\n"
            "*(Requires group admin access)*\n\n"
            "1. Visit [droptracker.io/account/players](https://www.droptracker.io/account/players) "
            "and log in with Discord.\n"
            "2. Click your group's name in the side navigation (desktop) or hamburger menu (mobile).\n"
            "3. Open the **Configuration** tab and set up your tracking preferences.\n\n"
            "From there you can configure:\n"
            "• **Notification channel** — where individual drop announcements are posted\n"
            "• **Lootboard channel** — where the periodic loot leaderboard image is posted\n"
            "• **Minimum drop value** — filters out low-value drops from notifications\n\n"
            "Your first loot leaderboard will typically post within 5 minutes of configuring. "
            "Drop tracking is retroactive — members' drops are counted from the date they installed the plugin.\n\n"
            "> The bot needs **Send Messages, Embed Links, and Attach Files** in the channels you select "
            "— or grant it Administrator permissions.\n\n"
            "Don't see your group in the navigation? Let me know your group name and I can look it up."
        ),
    },
    {
        "keywords": [
            "wise old man", "wom", "wiseoldman", "wom integration", "wom group",
            "wom sync", "wom id", "wom group id", "sync members",
        ],
        "response": (
            "**Wise Old Man (WOM) Integration**\n\n"
            "DropTracker uses WOM for player verification and group membership management:\n\n"
            "• **Player verification** — When you use `/claim-rsn`, your account is looked up via WOM.\n"
            "• **Automatic group sync** — Membership is synced from WOM at least every 2 hours. "
            "WOM is the definitive source for group rosters in our database.\n"
            "• **Manual sync** — Group admins can trigger an immediate sync with `/sync-wom` "
            "(1-hour cooldown) or `/force-group-sync` to bypass it.\n\n"
            "**Player not appearing after joining the WOM group?**\n"
            "Occasionally WOM doesn't pick up new characters automatically, especially fresh accounts. "
            "Visit [wiseoldman.net](https://www.wiseoldman.net), search for the player name, "
            "and press **Update** to register them manually.\n\n"
            "**Finding your WOM Group ID:** Go to wiseoldman.net → search for your group → "
            "the 3–6 digit number in the URL is your Group ID."
        ),
    },
    {
        "keywords": [
            "collection log", "clog", "collection log tracking", "log slots",
            "clog notifications", "collection log not tracking", "clog not working",
        ],
        "response": (
            "**Collection Log Tracking**\n\n"
            "DropTracker tracks collection log completions automatically — "
            "no additional RuneLite plugins are required beyond DropTracker itself.\n\n"
            "• New slots are recorded the first time you obtain an item in-game.\n"
            "• Your total log slot count is visible on your player profile at "
            "[droptracker.io](https://www.droptracker.io).\n"
            "• Notifications can be posted to Discord when you complete a section.\n\n"
            "**Slots not recording?**\n"
            "Make sure the **in-game collection log popup notification** is enabled in your "
            "OSRS game settings (Options → All Settings → Notifications). "
            "The plugin relies on this popup to detect new log entries."
        ),
    },
    {
        "keywords": [
            "personal best", "pb", "boss time", "kill time", "pb tracking",
            "pb not tracking", "boss pb", "import pb", "poh",
        ],
        "response": (
            "**Personal Best Tracking**\n\n"
            "PBs are tracked automatically — no additional RuneLite plugins are required.\n\n"
            "• A new PB is recorded whenever you beat your previous best kill time.\n"
            "• Groups that support the project can host a **Hall of Fame** displaying their members' "
            "top personal bests in a ranked list.\n"
            "• PB notifications can be posted to Discord — configure this in your group's settings at "
            "[droptracker.io/account/players](https://www.droptracker.io/account/players).\n\n"
            "**Want to import your existing PBs?**\n"
            "Visit your **Player-Owned House (POH) Achievement Gallery** in-game and open the "
            "**Times** tab while the DropTracker plugin is active. "
            "This will submit all your pre-existing personal bests to our database."
        ),
    },
    {
        "keywords": [
            "premium", "upgrade", "subscription", "support the project",
            "premium features", "how to support", "hall of fame", "upgrades",
        ],
        "response": (
            "**Premium Features & Supporting DropTracker**\n\n"
            "DropTracker is a community-driven project. Groups can unlock premium features by "
            "upgrading their account at "
            "[droptracker.io/groups/upgrades](https://www.droptracker.io/groups/upgrades).\n\n"
            "Premium groups receive early access to new features alongside exclusive functionalities "
            "like the **Hall of Fame** — a ranked display of your group's top personal bests.\n\n"
            "Without the continued support of our premium groups, ongoing development would not be possible."
        ),
    },
    {
        "keywords": [
            "combat achievement", "ca", "combat achievements", "ca tracking",
            "combat achievement not tracking", "combat tasks",
        ],
        "response": (
            "**Combat Achievement Tracking**\n\n"
            "DropTracker records combat achievement completions automatically via the plugin "
            "— no additional plugins required.\n\n"
            "• CAs are submitted as you complete them in-game.\n"
            "• Your CA progress is visible on your player profile at "
            "[droptracker.io](https://www.droptracker.io).\n"
            "• Group leaderboards can highlight your top CA completers."
        ),
    },
    {
        "keywords": [
            "group setup", "clan setup", "how to set up a group", "create group",
            "add my clan", "register clan", "set up clan", "create-group", "new group",
        ],
        "response": (
            "**Setting Up a Group/Clan**\n"
            "*(Requires Discord server admin)*\n\n"
            "**Prerequisites:**\n"
            "1. A [WiseOldMan group](https://wiseoldman.net/groups) — "
            "[create one here](https://wiseoldman.net/groups/create) if needed.\n"
            "2. Administrator permissions in your Discord server.\n"
            "3. The DropTracker bot "
            "[invited to your server](https://discord.com/oauth2/authorize?client_id=1172933457010245762&permissions=8&scope=bot).\n\n"
            "**Steps:**\n"
            "1. Run `/create-group` in your server and enter your WOM Group ID (from the WOM group URL).\n"
            "2. Visit [droptracker.io/account/players](https://www.droptracker.io/account/players), "
            "click your group name, and open the **Configuration** tab to finish setup.\n"
            "3. Set a **notification channel**, **lootboard channel**, and **minimum drop value**.\n\n"
            "Members need to install the DropTracker plugin and use `/claim-rsn` to associate "
            "their accounts with Discord."
        ),
    },
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def match_knowledge(query: str) -> Optional[KnowledgeResponse]:
    """
    Returns a predefined response if the query matches a known topic, else None.

    Scores each entry by counting keyword hits in the lowercased query.
    The response may be a plain str or a route dict (for component-based replies).
    """
    query_lower = query.lower()
    best: Optional[KnowledgeResponse] = None
    best_score = 0

    for entry in KNOWLEDGE_BASE:
        score = sum(1 for kw in entry["keywords"] if kw in query_lower)
        if score > best_score:
            best_score = score
            best = entry["response"]

    return best if best_score > 0 else None


def get_routed_response(custom_id: str) -> Optional[str]:
    """Returns the pre-written text for a routing button click, or None."""
    return ROUTED_RESPONSES.get(custom_id)

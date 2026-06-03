"""
Static knowledge base for DropTracker support.

Entries are matched against user queries using keyword scoring.
These are checked before hitting the Claude API to provide instant
answers to common questions without consuming API tokens.
"""

from typing import Optional

KNOWLEDGE_BASE = [
    {
        "keywords": [
            "how to set up", "how do i set up", "setup", "getting started",
            "get started", "install plugin", "runelite plugin", "how to install",
            "how do i start", "where do i begin", "first time"
        ],
        "response": (
            "**Getting Started with DropTracker**\n\n"
            "1. **Install the RuneLite Plugin** — Search for \"DropTracker\" in the RuneLite Plugin Hub and install it.\n"
            "2. **Register at droptracker.io** — Create an account to get your API key.\n"
            "3. **Configure the Plugin** — Open plugin settings in RuneLite and paste your API key.\n"
            "4. **Claim your RSN** — Use `/link` in a DropTracker Discord server to link your OSRS account.\n"
            "5. **Start Playing** — Drops are submitted automatically as you earn them!\n\n"
            "For clan/group setup, use `/group setup` in your server (requires admin)."
        ),
    },
    {
        "keywords": [
            "claim rsn", "link account", "verify rsn", "register username",
            "link my account", "link rsn", "how to claim", "connect account"
        ],
        "response": (
            "**Linking Your RSN (RuneScape Name)**\n\n"
            "1. Use the `/link` command in a DropTracker-enabled Discord server.\n"
            "2. You'll be prompted to verify via **Wise Old Man (WOM)** — your account must be on WOM first.\n"
            "3. Once verified, drops will be associated with your Discord account.\n\n"
            "If WOM doesn't have your account: visit https://wiseoldman.net, search your username, "
            "and update it — then try `/link` again."
        ),
    },
    {
        "keywords": [
            "not tracking", "drops not showing", "missing drops", "drops not working",
            "not recording", "drops disappearing", "plugin not working", "not submitting"
        ],
        "response": (
            "**Drops Not Tracking? Troubleshooting Steps:**\n\n"
            "• **Plugin enabled?** Check RuneLite sidebar — ensure the DropTracker plugin is active.\n"
            "• **API key set?** Open plugin config and confirm your API key is entered.\n"
            "• **Drop value threshold** — The plugin filters drops below a minimum GP value. Check settings.\n"
            "• **Account linked?** Make sure your RSN is claimed with `/link`.\n"
            "• **NPC not tracked?** Some NPCs may be missing — report them on the website.\n"
            "• **Internet/firewall?** The plugin needs outbound HTTPS access to api.droptracker.io.\n\n"
            "Still stuck? Share the NPC name and item — I can check our database."
        ),
    },
    {
        "keywords": [
            "what is droptracker", "about droptracker", "what does droptracker do",
            "explain droptracker", "how does droptracker work"
        ],
        "response": (
            "**About DropTracker**\n\n"
            "DropTracker is an OSRS (Old School RuneScape) drop tracking ecosystem:\n\n"
            "• **RuneLite Plugin** — Automatically submits drops, PBs, and collection log entries.\n"
            "• **Discord Bot** — Posts drop notifications to clan channels, runs leaderboards.\n"
            "• **Web Dashboard** — View detailed stats, leaderboards, and history at droptracker.io.\n"
            "• **Group/Clan Support** — Clan-wide leaderboards, custom notification thresholds, WOM sync.\n"
            "• **Wise Old Man Integration** — Player verification and group member syncing.\n\n"
            "It's designed for OSRS clans who want visibility into their loot and achievements."
        ),
    },
    {
        "keywords": [
            "loot board", "lootboard", "leaderboard setup", "notification channel",
            "set up notifications", "drop notifications", "configure notifications"
        ],
        "response": (
            "**Setting Up Loot Boards & Notifications**\n\n"
            "*(Requires group admin permissions)*\n\n"
            "1. Use `/group setup` to open the group configuration wizard.\n"
            "2. Set a **notification channel** — where drop announcements are posted.\n"
            "3. Set your **minimum drop value** — filters out low-value drops.\n"
            "4. Set a **lootboard channel** — where the periodic leaderboard image is posted.\n\n"
            "The bot needs these permissions in those channels: **Send Messages, Embed Links, Attach Files**."
        ),
    },
    {
        "keywords": [
            "wise old man", "wom", "wiseoldman", "wom integration", "wom group",
            "wom sync", "wom id"
        ],
        "response": (
            "**Wise Old Man (WOM) Integration**\n\n"
            "DropTracker uses WOM for player verification and group management:\n\n"
            "• **Player verification** — RSNs are verified through WOM when you use `/link`.\n"
            "• **Group sync** — Set your WOM group ID in `/group setup` to auto-import members.\n"
            "• **Update required** — If your RSN is new on WOM, update it at wiseoldman.net first.\n\n"
            "To find your WOM group ID: go to wiseoldman.net → your group → the URL contains the ID."
        ),
    },
    {
        "keywords": [
            "api key", "plugin token", "authentication", "token", "where is my api key",
            "find api key", "api token"
        ],
        "response": (
            "**API Keys & Authentication**\n\n"
            "• **Plugin API Key** — Generated at droptracker.io after registering. Enter it in your RuneLite plugin config.\n"
            "• **Group API Key** — Available to group admins on the group management page.\n"
            "• **Discord OAuth** — Use `/link` to authenticate your Discord with your RSN.\n\n"
            "⚠️ Your API key is private — never share it in public channels."
        ),
    },
    {
        "keywords": [
            "collection log", "clog", "collection log tracking", "log slots",
            "clog notifications", "collection log not tracking"
        ],
        "response": (
            "**Collection Log Tracking**\n\n"
            "DropTracker tracks collection log completions automatically via the RuneLite plugin.\n\n"
            "• Make sure the **Collection Log** plugin is enabled in RuneLite alongside DropTracker.\n"
            "• New slots are recorded when you obtain the item for the first time.\n"
            "• Your log slot count is visible on your player profile at droptracker.io.\n"
            "• Notifications can be sent to Discord when you complete a collection log section."
        ),
    },
    {
        "keywords": [
            "personal best", "pb", "boss time", "kill time", "pb tracking",
            "boss timer", "pb not tracking"
        ],
        "response": (
            "**Personal Best Tracking**\n\n"
            "PBs are tracked automatically via the RuneLite boss timer integration.\n\n"
            "• Ensure the **Boss Timer** plugin is enabled in RuneLite.\n"
            "• PBs are submitted when you beat your previous best time.\n"
            "• View your PBs on the droptracker.io player profile page.\n"
            "• Discord notifications can be sent for new PBs — configure in `/group setup`."
        ),
    },
    {
        "keywords": [
            "patreon", "premium", "subscription", "donate", "support the project",
            "premium features", "how to support"
        ],
        "response": (
            "**Supporting DropTracker**\n\n"
            "DropTracker is a community project. Check the DropTracker website or the #announcements channel "
            "for current information about premium features and how to support the project's development."
        ),
    },
    {
        "keywords": [
            "combat achievement", "ca", "combat achievements", "ca tracking",
            "combat achievement not tracking"
        ],
        "response": (
            "**Combat Achievement Tracking**\n\n"
            "DropTracker records combat achievement completions via the RuneLite plugin.\n\n"
            "• CAs are submitted automatically when completed in-game.\n"
            "• View your CA progress on your player profile at droptracker.io.\n"
            "• Group leaderboards can show top CA completers."
        ),
    },
    {
        "keywords": [
            "group setup", "clan setup", "how to set up a group", "create group",
            "add my clan", "register clan", "set up clan"
        ],
        "response": (
            "**Setting Up a Group/Clan**\n\n"
            "*(Requires Discord server admin)*\n\n"
            "1. Invite the DropTracker bot to your server.\n"
            "2. Use `/group setup` to start the configuration wizard.\n"
            "3. Set your **WOM group ID** to sync members automatically.\n"
            "4. Configure **notification channels**, **lootboard channels**, and **min value thresholds**.\n"
            "5. Share the DropTracker plugin with your members so they can start submitting drops.\n\n"
            "Members need to use `/link` to associate their OSRS accounts with Discord."
        ),
    },
]


def match_knowledge(query: str) -> Optional[str]:
    """
    Returns a predefined answer if the query matches a known topic.

    Scores each entry by counting how many of its keywords appear in the
    lowercased query. Returns the best match if any keyword hits, else None.
    """
    query_lower = query.lower()
    best_response: Optional[str] = None
    best_score = 0

    for entry in KNOWLEDGE_BASE:
        score = sum(1 for kw in entry["keywords"] if kw in query_lower)
        if score > best_score:
            best_score = score
            best_response = entry["response"]

    return best_response if best_score > 0 else None

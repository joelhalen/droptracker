#!/usr/bin/env python3
"""
Bulk Redis Update Script

Provides a command-line interface for forcing Redis refreshes from the database.

Usage:
    # Update all players with current month drops (original behaviour)
    python bulk_redis_update.py

    # Update every player in the database, regardless of partition
    python bulk_redis_update.py --all

    # Update every player in a specific group
    python bulk_redis_update.py --group 42

    # Update specific players by ID
    python bulk_redis_update.py --players 795,123,456

    # Dry run (show count without updating)
    python bulk_redis_update.py --dry-run
    python bulk_redis_update.py --all --dry-run
    python bulk_redis_update.py --group 42 --dry-run

    # Custom batch size
    python bulk_redis_update.py --batch-size 25
"""

import argparse
import sys
from datetime import datetime
from services.redis_updates import BulkRedisUpdater, loot_tracker
from db.models.base import get_fresh_session


def progress_callback(progress):
    """Generic progress callback"""
    total = progress.get('total_players', 0)
    done = progress.get('completed_players', 0)
    percent = (done / total * 100) if total else 0
    batch = progress.get('batch', '')
    total_batches = progress.get('total_batches', '')
    batch_str = f" - Batch {batch}/{total_batches}" if batch else ""
    print(f"Progress: {done}/{total} ({percent:.1f}%){batch_str} "
          f"- Success: {progress.get('successful', 0)}, Failed: {progress.get('failed', 0)}")


def print_summary(result: dict):
    print("\n" + "=" * 60)
    print("BULK UPDATE SUMMARY")
    print("=" * 60)
    print(f"Started at:  {result['started_at']}")
    print(f"Completed:   {result['completed_at']}")
    print(f"Duration:    {result['duration_seconds']:.2f}s")
    print(f"Total:       {result['total_players']}")
    print(f"Successful:  {result['successful_updates']}")
    print(f"Failed:      {result['failed_updates']}")

    if result['errors']:
        shown = result['errors'][:10]
        print(f"\nErrors ({len(result['errors'])}):")
        for i, err in enumerate(shown, 1):
            print(f"  {i}. {err}")
        if len(result['errors']) > 10:
            print(f"  ... and {len(result['errors']) - 10} more")

    rate = (result['successful_updates'] / result['total_players'] * 100
            if result['total_players'] else 0)
    print(f"\nSuccess rate: {rate:.1f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Bulk Redis update for DropTracker players",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        '--all',
        action='store_true',
        help='Update every player in the database (global rebuild)',
    )
    scope.add_argument(
        '--group',
        type=int,
        metavar='GROUP_ID',
        help='Update every player in the specified group',
    )
    scope.add_argument(
        '--players',
        type=str,
        metavar='ID1,ID2,...',
        help='Comma-separated list of specific player IDs to update',
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show how many players would be updated without actually updating',
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Players per batch (default: 50)',
    )
    parser.add_argument(
        '--fetch-chunk-size',
        type=int,
        default=50000,
        help='Player IDs fetched per DB query when scanning (default: 50000)',
    )

    args = parser.parse_args()

    if args.fetch_chunk_size <= 0:
        parser.error("--fetch-chunk-size must be greater than zero")
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than zero")

    bulk_updater = BulkRedisUpdater(
        batch_size=args.batch_size,
        player_fetch_chunk_size=args.fetch_chunk_size,
    )

    try:
        # ------------------------------------------------------------------ #
        # --all: rebuild every player in the database                        #
        # ------------------------------------------------------------------ #
        if args.all:
            if args.dry_run:
                list_session = get_fresh_session()
                try:
                    count = list(bulk_updater._stream_all_player_ids(list_session, args.fetch_chunk_size))
                    print(f"DRY RUN: Would update {len(count)} players (global)")
                finally:
                    list_session.close()
                return 0

            print("Starting global Redis update for ALL players...")
            result = bulk_updater.force_update_all_players(
                progress_callback=progress_callback,
            )

        # ------------------------------------------------------------------ #
        # --group: rebuild every player in a group                           #
        # ------------------------------------------------------------------ #
        elif args.group is not None:
            group_id = args.group
            if args.dry_run:
                from db import user_group_association
                list_session = get_fresh_session()
                try:
                    count = list_session.query(user_group_association.c.player_id).filter(
                        user_group_association.c.group_id == group_id,
                        user_group_association.c.player_id != None,
                    ).count()
                    print(f"DRY RUN: Would update {count} players in group {group_id}")
                finally:
                    list_session.close()
                return 0

            print(f"Starting Redis update for all players in group {group_id}...")
            result = loot_tracker.force_update_group(
                group_id,
                progress_callback=progress_callback,
            )

        # ------------------------------------------------------------------ #
        # --players: rebuild specific player IDs                             #
        # ------------------------------------------------------------------ #
        elif args.players:
            try:
                player_ids = [int(pid.strip()) for pid in args.players.split(',')]
            except ValueError:
                print("Error: --players expects comma-separated integers.")
                return 1

            if args.dry_run:
                print(f"DRY RUN: Would update {len(player_ids)} specific players:")
                for pid in player_ids:
                    print(f"  - Player ID: {pid}")
                return 0

            print(f"Starting Redis update for {len(player_ids)} specific players...")
            result = bulk_updater.force_update_specific_players(
                player_ids,
                progress_callback=progress_callback,
            )

        # ------------------------------------------------------------------ #
        # default: current-month players only                                #
        # ------------------------------------------------------------------ #
        else:
            if args.dry_run:
                list_session = get_fresh_session()
                try:
                    players = bulk_updater.get_players_with_current_month_drops(list_session)
                    partition = datetime.now().year * 100 + datetime.now().month
                    print(f"DRY RUN: Would update {len(players)} players "
                          f"with drops in partition {partition}")
                finally:
                    list_session.close()
                return 0

            print("Starting Redis update for players with current-month drops...")
            result = bulk_updater.force_update_all_current_month_players(
                progress_callback=progress_callback,
            )

        print_summary(result)
        return 0 if result['failed_updates'] == 0 else 1

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        return 1
    except Exception as e:
        print(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

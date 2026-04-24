#!/usr/bin/env python3
"""
Database cleanup script - removes entries for files that no longer exist on disk.
"""

import logging
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

DB_SEARCH_PATHS = [
    Path.home() / ".votify",
    Path.home() / ".votify" / "database",
    Path.home() / ".config" / "votify",
    Path.home() / ".local" / "share" / "votify",
    Path("/root/votify-dev"),
    Path.cwd(),
    Path.cwd().parent,
]


def find_databases() -> list[Path]:
    """Search common locations for votify SQLite databases."""
    found = []
    seen = set()

    for search_path in DB_SEARCH_PATHS:
        if not search_path.exists():
            continue
        for db_file in search_path.rglob("*.db"):
            resolved = db_file.resolve()
            if resolved not in seen:
                seen.add(resolved)
                found.append(db_file)
                logger.info(f"Found database: {db_file}")

    return found


def cleanup_database(db_path: Path, dry_run: bool = False) -> dict:
    """
    Check all database entries and remove those where the file no longer exists.
    Uses raw sqlite3 directly - no dependency on votify Database class.
    """
    logger.info(f"Opening database: {db_path}")
    logger.info(
        f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE (entries will be deleted)'}"
    )

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        # --- Fetch all entries ---
        cursor.execute("SELECT id, path FROM media")
        all_entries: list[tuple[str, str]] = cursor.fetchall()
        total = len(all_entries)

        logger.info(f"Total entries in database: {total}")

        if total == 0:
            logger.info("Database is empty, nothing to clean up.")
            return {"total": 0, "found": 0, "missing": 0, "removed": 0}

        # --- Check each file ---
        missing_ids: list[str] = []
        missing_paths: list[str] = []

        for i, (media_id, path) in enumerate(all_entries, 1):
            # Progress every 1000 entries
            if i % 1000 == 0:
                logger.info(f"  Checking... {i}/{total} ({len(missing_ids)} missing so far)")

            if not Path(path).exists():
                missing_ids.append(media_id)
                missing_paths.append(path)

        found_count = total - len(missing_ids)

        logger.info(f"Files found on disk:   {found_count}/{total}")
        logger.info(f"Files missing on disk: {len(missing_ids)}/{total}")

        if not missing_ids:
            logger.info("No missing files found. Database is clean.")
            return {"total": total, "found": found_count, "missing": 0, "removed": 0}

        # --- Report missing entries ---
        logger.info("Missing files:")
        for media_id, path in zip(missing_ids, missing_paths):
            logger.info(f"  [{media_id}] {path}")

        # --- Remove if not dry run ---
        removed = 0
        if dry_run:
            logger.info(f"DRY RUN: Would remove {len(missing_ids)} entries.")
        else:
            logger.info(f"Removing {len(missing_ids)} entries...")
            placeholders = ",".join("?" * len(missing_ids))
            cursor.execute(
                f"DELETE FROM media WHERE id IN ({placeholders})", missing_ids
            )
            conn.commit()
            removed = len(missing_ids)
            logger.info(f"Done. Entries remaining: {total - removed}")

        return {
            "total": total,
            "found": found_count,
            "missing": len(missing_ids),
            "removed": removed,
        }

    finally:
        conn.close()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Remove database entries for media files that no longer exist on disk."
    )
    parser.add_argument(
        "db_path",
        type=Path,
        nargs="?",
        help="Path to the SQLite database file. If omitted, common locations are scanned.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report missing entries without deleting them",
    )
    parser.add_argument(
        "--scan",
        action="store_true",
        help="Scan for databases and show found locations, then exit",
    )
    args = parser.parse_args()

    # --- Scan mode ---
    if args.scan:
        logger.info("Scanning for databases...")
        dbs = find_databases()
        if not dbs:
            logger.info("No databases found.")
        else:
            logger.info(f"Found {len(dbs)} database(s):")
            for db in dbs:
                logger.info(f"  {db.resolve()}")
        return

    # --- Determine which DB(s) to process ---
    if args.db_path:
        if not args.db_path.exists():
            logger.error(f"Database file not found: {args.db_path}")
            sys.exit(1)
        db_paths = [args.db_path]
    else:
        logger.info("No database path given - scanning for databases...")
        db_paths = find_databases()
        if not db_paths:
            logger.error(
                "No databases found. Please provide the path manually:\n"
                "  python db_update.py /root/.votify/database/votify.db"
            )
            sys.exit(1)

        if len(db_paths) > 1:
            print("\nFound multiple databases:")
            for i, p in enumerate(db_paths):
                print(f"  [{i}] {p.resolve()}")
            choice = input("Which one to clean? (number, or 'all'): ").strip()
            if choice == "all":
                pass
            elif choice.isdigit() and int(choice) < len(db_paths):
                db_paths = [db_paths[int(choice)]]
            else:
                logger.error("Invalid choice.")
                sys.exit(1)

    # --- Run cleanup ---
    total_stats = {"total": 0, "found": 0, "missing": 0, "removed": 0}

    for db_path in db_paths:
        print(f"\n{'=' * 60}")
        result = cleanup_database(db_path=db_path, dry_run=args.dry_run)
        for key in total_stats:
            total_stats[key] += result[key]

    if len(db_paths) > 1:
        print(f"\n{'=' * 60}")
        logger.info("Overall summary:")
        logger.info(f"  Databases processed: {len(db_paths)}")
        logger.info(f"  Total entries:       {total_stats['total']}")
        logger.info(f"  Found on disk:       {total_stats['found']}")
        logger.info(f"  Missing:             {total_stats['missing']}")
        logger.info(f"  Removed:             {total_stats['removed']}")


if __name__ == "__main__":
    main()

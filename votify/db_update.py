#!/usr/bin/env python3
"""
Database cleanup script - removes entries for files that no longer exist on disk.
Compatible with both old and new Database class versions.
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
    Path("/root/votify-dev/votify"),
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


class UniversalDatabase:
    """
    Wrapper that works with both old and new Database class versions.
    Falls back to direct sqlite3 if the Database class is not available.
    """

    def __init__(self, path: Path):
        self.path = path
        self._db = None
        self._conn = None
        self._use_raw_sqlite = False
        self._setup(path)

    def _setup(self, path: Path):
        """Try to use the votify Database class, fall back to raw sqlite3."""
        db_class = self._try_import_database()

        if db_class:
            try:
                self._db = db_class(path)
                # Check if context manager is supported (new version)
                if not hasattr(self._db, "__enter__"):
                    # Old version - add context manager manually
                    self._db.__enter__ = lambda: self._db
                    self._db.__exit__ = lambda *a: self._db.close()
                logger.debug(f"Using votify Database class from: {db_class.__module__}")
                return
            except Exception as e:
                logger.warning(f"Could not initialize Database class: {e}, falling back to raw sqlite3")

        # Fallback: raw sqlite3
        logger.debug("Using raw sqlite3 connection")
        self._use_raw_sqlite = True
        self._conn = sqlite3.connect(str(path))

    def _try_import_database(self):
        """Try all known import paths for the Database class."""
        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parent

        for path in [str(project_root), str(script_dir)]:
            if path not in sys.path:
                sys.path.insert(0, path)

        # Try known import paths
        import_attempts = [
            ("votify.cli.database", "Database"),
            ("cli.database", "Database"),
        ]

        for module_path, class_name in import_attempts:
            try:
                import importlib
                module = importlib.import_module(module_path)
                cls = getattr(module, class_name)
                return cls
            except (ImportError, AttributeError):
                continue

        # Try loading directly from file
        candidates = [
            script_dir / "cli" / "database.py",
            project_root / "votify" / "cli" / "database.py",
        ]
        for candidate in candidates:
            if candidate.exists():
                try:
                    import importlib.util
                    spec = importlib.util.spec_from_file_location("database", candidate)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    return module.Database
                except Exception:
                    continue

        return None

    def get_all_entries(self) -> list[tuple[str, str]]:
        """Fetch all (id, path) entries from the database."""
        if self._use_raw_sqlite:
            cursor = self._conn.cursor()
            cursor.execute("SELECT id, path FROM media")
            return cursor.fetchall()
        else:
            self._db.cursor.execute("SELECT id, path FROM media")
            return self._db.cursor.fetchall()

    def get_total_count(self) -> int:
        """Get total number of entries."""
        if self._use_raw_sqlite:
            cursor = self._conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM media")
            return cursor.fetchone()[0]
        else:
            # New version has get_stats(), old version doesn't
            if hasattr(self._db, "get_stats"):
                return self._db.get_stats()["total_entries"]
            else:
                self._db.cursor.execute("SELECT COUNT(*) FROM media")
                return self._db.cursor.fetchone()[0]

    def remove_batch(self, media_ids: list[str]) -> None:
        """Remove multiple entries - works with old and new Database class."""
        if not media_ids:
            return

        if self._use_raw_sqlite:
            placeholders = ",".join("?" * len(media_ids))
            self._conn.execute(f"DELETE FROM media WHERE id IN ({placeholders})", media_ids)
            self._conn.commit()
        else:
            # New version has remove_batch(), old version only has remove()
            if hasattr(self._db, "remove_batch"):
                self._db.remove_batch(media_ids)
            else:
                logger.debug("Old Database class detected - using single remove() calls")
                for media_id in media_ids:
                    self._db.remove(media_id)

    def close(self):
        if self._use_raw_sqlite and self._conn:
            self._conn.close()
        elif self._db:
            self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def cleanup_database(db_path: Path, dry_run: bool = False) -> dict:
    """
    Check all database entries and remove those where the file no longer exists.

    Args:
        db_path: Path to the SQLite database file
        dry_run: If True, only report missing files without deleting them

    Returns:
        dict with cleanup statistics
    """
    logger.info(f"Opening database: {db_path}")
    logger.info(
        f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE (entries will be deleted)'}"
    )

    with UniversalDatabase(db_path) as db:
        # --- Fetch all entries ---
        all_entries = db.get_all_entries()
        total = db.get_total_count()

        logger.info(f"Total entries in database: {total}")

        if total == 0:
            logger.info("Database is empty, nothing to clean up.")
            return {"total": 0, "found": 0, "missing": 0, "removed": 0}

        # --- Check each entry ---
        missing_ids: list[str] = []
        missing_paths: list[str] = []

        for media_id, path in all_entries:
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

        # --- Remove missing entries (unless dry run) ---
        removed = 0
        if dry_run:
            logger.info(f"DRY RUN: Would remove {len(missing_ids)} entries.")
        else:
            logger.info(f"Removing {len(missing_ids)} entries...")
            db.remove_batch(missing_ids)
            removed = len(missing_ids)
            logger.info("Cleanup complete.")
            logger.info(f"Entries remaining in database: {total - removed}")

        return {
            "total": total,
            "found": found_count,
            "missing": len(missing_ids),
            "removed": removed,
        }


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
                "  python db_update.py /path/to/votify.db"
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

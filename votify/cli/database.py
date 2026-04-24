import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path, check_same_thread=False)
        # Enable WAL mode for better concurrent performance
        self.connection.execute("PRAGMA journal_mode=WAL")
        # Increase cache size (in KB) - 64MB cache
        self.connection.execute("PRAGMA cache_size=-64000")
        # Use memory for temp storage
        self.connection.execute("PRAGMA temp_store=MEMORY")
        # Faster synchronization (still safe)
        self.connection.execute("PRAGMA synchronous=NORMAL")
        
        self.cursor = self.connection.cursor()
        self._create_tables()
        self._create_indexes()
        
        # In-memory cache for recently accessed items
        self._cache: Dict[str, Optional[str]] = {}
        self._cache_size = 10000  # Adjust based on memory availability

    def _create_tables(self) -> None:
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS media (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL
            )
            """
        )
        self.connection.commit()

    def _create_indexes(self) -> None:
        """Create indexes for faster lookups"""
        self.cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_media_id ON media(id)
            """
        )
        self.connection.commit()

    def get(self, media_id: str) -> str | None:
        """Get single media path with caching"""
        # Check cache first
        if media_id in self._cache:
            return self._cache[media_id]
        
        self.cursor.execute("SELECT path FROM media WHERE id = ?", (media_id,))
        row = self.cursor.fetchone()
        result = row[0] if row else None
        
        # Update cache
        self._update_cache(media_id, result)
        
        return result

    def get_batch(self, media_ids: List[str]) -> Dict[str, str]:
        """
        Get multiple media paths in one query - MUCH faster for playlists
        Returns dict of {media_id: path} for found items only
        """
        if not media_ids:
            return {}
        
        # Check cache first
        cached_results = {}
        uncached_ids = []
        
        for media_id in media_ids:
            if media_id in self._cache:
                if self._cache[media_id] is not None:
                    cached_results[media_id] = self._cache[media_id]
            else:
                uncached_ids.append(media_id)
        
        # If all items were cached, return immediately
        if not uncached_ids:
            logger.debug(f"All {len(media_ids)} items found in cache")
            return cached_results
        
        # Batch query for uncached items
        placeholders = ','.join('?' * len(uncached_ids))
        query = f"SELECT id, path FROM media WHERE id IN ({placeholders})"
        
        self.cursor.execute(query, uncached_ids)
        rows = self.cursor.fetchall()
        
        db_results = {row[0]: row[1] for row in rows}
        
        # Update cache for all queried items (including non-existent ones)
        for media_id in uncached_ids:
            path = db_results.get(media_id)
            self._update_cache(media_id, path)
        
        logger.debug(f"Batch query: {len(cached_results)} cached, {len(db_results)} from DB, {len(uncached_ids) - len(db_results)} not found")
        
        # Combine cached and db results
        return {**cached_results, **db_results}

    def add(self, media_id: str, path: str) -> None:
        """Add single media entry"""
        self.cursor.execute(
            "INSERT OR REPLACE INTO media (id, path) VALUES (?, ?)",
            (media_id, path),
        )
        self.connection.commit()
        
        # Update cache
        self._update_cache(media_id, path)

    def add_batch(self, media_data: List[tuple[str, str]]) -> None:
        """
        Add multiple media entries in one transaction - MUCH faster
        media_data: List of (media_id, path) tuples
        """
        if not media_data:
            return
        
        self.cursor.executemany(
            "INSERT OR REPLACE INTO media (id, path) VALUES (?, ?)",
            media_data
        )
        self.connection.commit()
        
        # Update cache
        for media_id, path in media_data:
            self._update_cache(media_id, path)
        
        logger.debug(f"Added {len(media_data)} entries in batch")

    def remove(self, media_id: str) -> None:
        """Remove single media entry"""
        self.cursor.execute("DELETE FROM media WHERE id = ?", (media_id,))
        self.connection.commit()
        
        # Remove from cache
        self._cache.pop(media_id, None)

    def remove_batch(self, media_ids: List[str]) -> None:
        """Remove multiple media entries in one transaction"""
        if not media_ids:
            return
        
        placeholders = ','.join('?' * len(media_ids))
        query = f"DELETE FROM media WHERE id IN ({placeholders})"
        
        self.cursor.execute(query, media_ids)
        self.connection.commit()
        
        # Remove from cache
        for media_id in media_ids:
            self._cache.pop(media_id, None)

    def _update_cache(self, media_id: str, path: Optional[str]) -> None:
        """Update cache with size limit using simple FIFO"""
        if len(self._cache) >= self._cache_size:
            # Remove oldest entry (first item)
            first_key = next(iter(self._cache))
            del self._cache[first_key]
        
        self._cache[media_id] = path

    def clear_cache(self) -> None:
        """Clear the in-memory cache"""
        self._cache.clear()
        logger.debug("Cache cleared")

    def get_stats(self) -> Dict[str, int]:
        """Get database statistics"""
        self.cursor.execute("SELECT COUNT(*) FROM media")
        count = self.cursor.fetchone()[0]
        
        return {
            "total_entries": count,
            "cache_size": len(self._cache),
            "cache_limit": self._cache_size,
        }

    def close(self) -> None:
        """Close database connection"""
        self.connection.close()
        logger.debug("Database connection closed")

    def flat_filter(self, media_metadata: dict) -> str | None:
        """Check if media already exists in database"""
        media_id = media_metadata["uri"].split(":")[-1]
        return self.get(media_id)

    def __enter__(self):
        """Context manager support"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()

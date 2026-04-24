import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)


class DownloadTracker:
    """Track successful and failed downloads with detailed logging"""
    
    def __init__(self, log_path: str = "download_log.json"):
        self.log_path = Path(log_path)
        self.failed_downloads: List[Dict] = []
        self.successful_downloads: List[Dict] = []
        self.skipped_downloads: List[Dict] = []
        
        # Load existing log if available
        if self.log_path.exists():
            try:
                with open(self.log_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.failed_downloads = data.get('failed', [])
                    self.successful_downloads = data.get('successful', [])
                    self.skipped_downloads = data.get('skipped', [])
                logger.info(f"Loaded existing download log from {self.log_path}")
            except Exception as e:
                logger.warning(f"Could not load existing log: {e}")
    
    def add_failed(self, media_id: str, title: str, error: str, track_number: int = None):
        """Record a failed download"""
        entry = {
            "media_id": media_id,
            "title": title,
            "error": error,
            "timestamp": datetime.now().isoformat(),
            "track_number": track_number,
        }
        self.failed_downloads.append(entry)
        logger.error(f"[FAILED] Track {track_number or 'N/A'}: {title} - {error}")
        self._save()
    
    def add_successful(self, media_id: str, title: str, file_path: str, track_number: int = None):
        """Record a successful download"""
        entry = {
            "media_id": media_id,
            "title": title,
            "file_path": file_path,
            "timestamp": datetime.now().isoformat(),
            "track_number": track_number,
        }
        self.successful_downloads.append(entry)
        self._save()
    
    def add_skipped(self, media_id: str, title: str, reason: str, track_number: int = None):
        """Record a skipped download"""
        entry = {
            "media_id": media_id,
            "title": title,
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
            "track_number": track_number,
        }
        self.skipped_downloads.append(entry)
        logger.warning(f"[SKIPPED] Track {track_number or 'N/A'}: {title} - {reason}")
        self._save()
    
    def _save(self):
        """Save the log to disk"""
        try:
            data = {
                "failed": self.failed_downloads,
                "successful": self.successful_downloads,
                "skipped": self.skipped_downloads,
                "summary": {
                    "total_failed": len(self.failed_downloads),
                    "total_successful": len(self.successful_downloads),
                    "total_skipped": len(self.skipped_downloads),
                    "last_updated": datetime.now().isoformat(),
                }
            }
            with open(self.log_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Could not save download log: {e}")
    
    def get_failed_ids(self) -> List[str]:
        """Get list of failed media IDs for retry"""
        return [entry["media_id"] for entry in self.failed_downloads]
    
    def print_summary(self):
        """Print a summary of the download session"""
        logger.info("=" * 70)
        logger.info("DOWNLOAD SESSION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"✓ Successful: {len(self.successful_downloads)}")
        logger.info(f"⏭ Skipped: {len(self.skipped_downloads)}")
        logger.info(f"✗ Failed: {len(self.failed_downloads)}")
        logger.info(f"Log saved to: {self.log_path.absolute()}")
        logger.info("=" * 70)
        
        if self.failed_downloads:
            logger.info("\nFailed Downloads:")
            for entry in self.failed_downloads[-10:]:  # Show last 10
                logger.error(
                    f"  [{entry.get('track_number', 'N/A')}] {entry['title']}: {entry['error']}"
                )
            if len(self.failed_downloads) > 10:
                logger.info(f"  ... and {len(self.failed_downloads) - 10} more")

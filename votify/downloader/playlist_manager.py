import logging
from pathlib import Path
from typing import Dict, Set

logger = logging.getLogger(__name__)


class PlaylistManager:
    """
    Manages playlist file generation with proper UTF-8 encoding and no gaps.
    Collects all tracks and writes the complete playlist file at the end.
    ALWAYS creates M3U8 file, even if only skipped tracks.
    """

    def __init__(self):
        # Structure: {playlist_path: {track_number: relative_file_path}}
        self.playlists: Dict[str, Dict[int, str]] = {}
        self.playlist_totals: Dict[str, int] = {}

    def add_track(
        self,
        playlist_file_path: str,
        track_number: int,
        relative_path: str,
        total_tracks: int = None,
    ) -> None:
        """
        Register a track for a playlist.

        Args:
            playlist_file_path: Full path to the .m3u8 file
            track_number: Track position (1-indexed)
            relative_path: Relative path to the media file
            total_tracks: Total number of tracks in playlist (optional)
        """
        if playlist_file_path not in self.playlists:
            self.playlists[playlist_file_path] = {}

        self.playlists[playlist_file_path][track_number] = relative_path

        if total_tracks:
            self.playlist_totals[playlist_file_path] = total_tracks

        logger.debug(
            f"Added track {track_number} to playlist {Path(playlist_file_path).name}"
        )

    def write_playlist(self, playlist_file_path: str) -> None:
        """
        Write a complete playlist file without gaps, with proper UTF-8 encoding.
        ALWAYS creates the M3U8 file, even if empty or only skipped tracks.

        Args:
            playlist_file_path: Full path to the .m3u8 file
        """
        if playlist_file_path not in self.playlists:
            logger.debug(f"No tracks registered for playlist: {playlist_file_path}")
            return

        tracks = self.playlists[playlist_file_path]

        if not tracks:
            logger.debug(f"Playlist has no tracks: {playlist_file_path}")
            return

        # Determine the total number of tracks
        max_track_number = max(tracks.keys())
        expected_total = self.playlist_totals.get(playlist_file_path, max_track_number)
        total_tracks = max(max_track_number, expected_total)

        # Create playlist file path and parent directories
        playlist_path = Path(playlist_file_path)
        playlist_path.parent.mkdir(parents=True, exist_ok=True)

        # Build complete track list (1-indexed)
        # Always writes ALL tracks, filling gaps with empty lines if needed
        playlist_lines = []
        missing_tracks = []

        for track_num in range(1, total_tracks + 1):
            if track_num in tracks:
                # Track exists - write the path
                playlist_lines.append(tracks[track_num] + "\n")
            else:
                # Track doesn't exist - write empty line
                # This keeps the sequential order (Track 1, 2, 3, etc.)
                playlist_lines.append("\n")
                missing_tracks.append(track_num)

        # Write M3U8 file with proper UTF-8 encoding
        try:
            with playlist_path.open("w", encoding="utf-8") as f:
                f.writelines(playlist_lines)

            # Log statistics
            downloaded_count = len(tracks)
            skipped_count = total_tracks - downloaded_count

            if missing_tracks:
                logger.info(
                    f"✓ M3U8 created: {playlist_path.name}"
                    f"\n  📥 Downloaded: {downloaded_count}/{total_tracks}"
                    f"\n  ⏭️  Skipped: {skipped_count}/{total_tracks}"
                    f"\n  📄 Total lines: {total_tracks}"
                )
            else:
                logger.info(
                    f"✓ M3U8 created: {playlist_path.name} "
                    f"({downloaded_count}/{total_tracks} tracks)"
                )

        except Exception as e:
            logger.error(f"Failed to write playlist {playlist_file_path}: {e}")
            raise

    def write_all_playlists(self) -> None:
        """
        Write all registered playlists to disk.
        ALWAYS creates M3U8 files for every playlist with tracks registered.
        """
        if not self.playlists:
            logger.debug("No playlists to write")
            return

        written_count = 0
        failed_count = 0

        for playlist_file_path in self.playlists.keys():
            try:
                self.write_playlist(playlist_file_path)
                written_count += 1
            except Exception as e:
                logger.error(f"Error writing playlist {playlist_file_path}: {e}")
                failed_count += 1

        total = written_count + failed_count
        logger.info(f"✓ Created/Updated {written_count}/{total} M3U8 file(s)")

        if failed_count > 0:
            logger.warning(f"⚠ Failed to write {failed_count} playlist(s)")

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about managed playlists."""
        total_playlists = len(self.playlists)
        total_tracks = sum(len(tracks) for tracks in self.playlists.values())

        return {
            "total_playlists": total_playlists,
            "total_tracks": total_tracks,
        }

    def clear(self) -> None:
        """Clear all playlist data."""
        self.playlists.clear()
        self.playlist_totals.clear()

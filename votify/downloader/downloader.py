import logging
import shutil
from pathlib import Path
from typing import AsyncGenerator

from ..interface.enums import AutoMediaOption, MediaType
from .audio import SpotifyAudioDownloader
from .base import SpotifyBaseDownloader
from .constants import TEMP_PATH_TEMPLATE
from .enums import AudioDownloadMode, AudioRemuxMode, VideoRemuxMode
from .exceptions import (
    VotifyDependencyNotFound,
    VotifyMediaFileExists,
    VotifySyncedLyricsOnly,
)
from .playlist_manager import PlaylistManager
from .types import DownloadItem
from .video import SpotifyVideoDownloader

logger = logging.getLogger(__name__)


class SpotifyDownloader:
    def __init__(
        self,
        base: SpotifyBaseDownloader,
        audio: SpotifyAudioDownloader,
        video: SpotifyVideoDownloader,
        no_synced_lyrics_file: bool = False,
        save_playlist_file: bool = False,
        save_cover_file: bool = False,
        overwrite: bool = False,
        synced_lyrics_only: bool = False,
        skip_processing: bool = False,
        skip_cleanup: bool = False,
    ) -> None:
        self.base = base
        self.audio = audio
        self.video = video
        self.no_synced_lyrics_file = no_synced_lyrics_file
        self.save_playlist_file = save_playlist_file
        self.save_cover_file = save_cover_file
        self.overwrite = overwrite
        self.synced_lyrics_only = synced_lyrics_only
        self.skip_processing = skip_processing
        self.skip_cleanup = skip_cleanup

        # Playlist manager for proper playlist file generation
        self.playlist_manager = PlaylistManager()

    async def get_download_item(
        self,
        url: str | None = None,
        auto_media_option: AutoMediaOption | None = None,
    ) -> AsyncGenerator[DownloadItem | BaseException, None]:
        async for media in self.base.interface.get_media(url, auto_media_option):
            if isinstance(media, BaseException):
                yield media
                continue

            if media.tags.media_type in {
                MediaType.SONG,
                MediaType.PODCAST,
            }:
                yield self.audio.parse_item(media)
            elif media.tags.media_type in {
                MediaType.MUSIC_VIDEO,
                MediaType.PODCAST_VIDEO,
            }:
                yield self.video.parse_item(media)

    async def download(self, item: DownloadItem) -> None:
        """
        Download media item with comprehensive error handling.
        ALWAYS registers track with playlist manager - even for skipped files.
        Ensures M3U8 is created with ALL track types:
        - Only downloaded files
        - Only skipped files
        - Mix of downloaded and skipped files
        """
        file_already_existed = False

        try:
            # Check if file already exists BEFORE any processing
            if item.final_path and Path(item.final_path).exists() and not self.overwrite:
                file_already_existed = True
                logger.debug(f"File already exists: {item.final_path}")

            # ✨ CRITICAL: Register track with playlist manager FIRST
            # This ensures M3U8 is created even if ALL tracks are skipped
            if item.playlist_file_path and item.final_path and self.save_playlist_file:
                relative_path = self.base.get_playlist_relative_path(
                    item.playlist_file_path,
                    item.final_path,
                )

                # Get total tracks from playlist metadata if available
                total_tracks = None
                if hasattr(item.media, 'playlist_tags') and item.media.playlist_tags:
                    total_tracks = getattr(item.media.playlist_tags, 'track_total', None)

                self.playlist_manager.add_track(
                    playlist_file_path=item.playlist_file_path,
                    track_number=item.media.playlist_tags.track,
                    relative_path=relative_path,
                    total_tracks=total_tracks,
                )

                track_status = "SKIPPED" if file_already_existed else "downloading"
                logger.debug(
                    f"Registered with playlist manager: "
                    f"track {item.media.playlist_tags.track}/{total_tracks or '?'} ({track_status})"
                )

            # If file exists and we're not overwriting, skip download but track is already registered!
            if file_already_existed:
                raise VotifyMediaFileExists(item.final_path)

            # Continue with initial processing (cover, lyrics)
            await self._initial_processing(item)

            # Download the actual media file
            await self._download(item)

            # Final processing (move from temp to final location)
            await self._final_processing(item)

        finally:
            if not self.skip_cleanup and not file_already_existed:
                # Only cleanup if we actually attempted a download
                self._cleanup_temp(item.uuid_)

    async def _download(self, item: DownloadItem) -> None:
        if self.synced_lyrics_only:
            raise VotifySyncedLyricsOnly()

        if item.final_path and Path(item.final_path).exists() and not self.overwrite:
            raise VotifyMediaFileExists(item.final_path)

        if item.media.tags.media_type in {
            MediaType.SONG,
            MediaType.PODCAST,
        }:
            if (
                self.audio.download_mode == AudioDownloadMode.ARIA2C
                and not self.base.aria2c_full_path
            ):
                raise VotifyDependencyNotFound("aria2c")

            if (
                self.audio.download_mode == AudioDownloadMode.CURL
                and not self.base.curl_full_path
            ):
                raise VotifyDependencyNotFound("cURL")

            if (
                item.media.stream_info.audio_track.file_format == "mp4"
                and self.audio.remux_mode == AudioRemuxMode.FFMPEG
                or (
                    item.media.stream_info.audio_track.actual_file_format == "flac"
                    and item.media.stream_info.audio_track.file_format == "mp4"
                )
            ) and not self.base.ffmpeg_full_path:
                raise VotifyDependencyNotFound("ffmpeg")

            if (
                item.media.stream_info.audio_track.file_format == "mp4"
                and self.audio.remux_mode == AudioRemuxMode.MP4BOX
                and not self.base.mp4box_full_path
            ):
                raise VotifyDependencyNotFound("MP4Box")

            if (
                item.media.stream_info.audio_track.file_format == "mp4"
                and (
                    self.audio.remux_mode == AudioRemuxMode.MP4DECRYPT
                    or self.audio.remux_mode == AudioRemuxMode.MP4BOX
                )
                and not self.base.mp4decrypt_full_path
            ):
                raise VotifyDependencyNotFound("mp4decrypt")

            await self.audio.download(item)
        elif item.media.tags.media_type in {
            MediaType.MUSIC_VIDEO,
            MediaType.PODCAST_VIDEO,
        }:
            if (
                self.video.remux_mode == VideoRemuxMode.FFMPEG
                and not self.base.ffmpeg_full_path
            ):
                raise VotifyDependencyNotFound("ffmpeg")

            if (
                self.video.remux_mode == VideoRemuxMode.MP4BOX
                and not self.base.mp4box_full_path
            ):
                raise VotifyDependencyNotFound("MP4Box")

            if item.media.decryption_key:
                if (
                    item.media.stream_info.video_track.file_format == "mp4"
                    or item.media.stream_info.audio_track.file_format == "mp4"
                ) and not self.base.mp4decrypt_full_path:
                    raise VotifyDependencyNotFound("mp4decrypt")

                if (
                    item.media.stream_info.video_track.file_format == "webm"
                    or item.media.stream_info.audio_track.file_format == "webm"
                ) and not self.base.shaka_packager_full_path:
                    raise VotifyDependencyNotFound("Shaka Packager")

            await self.video.download(item)

    def _cleanup_temp(self, folder_tag: str) -> None:
        temp_path = Path(self.base.temp_path) / TEMP_PATH_TEMPLATE.format(folder_tag)
        if temp_path.exists() and temp_path.is_dir():
            shutil.rmtree(temp_path, ignore_errors=True)

    async def _initial_processing(self, item: DownloadItem) -> None:
        """
        Process cover art and lyrics for new downloads.
        Playlist registration is done in download() method.
        """
        if self.skip_processing:
            return

        # Cover and lyrics only for new files or when overwriting
        if item.cover_path and self.save_cover_file and item.media.cover_url:
            cover_bytes = await self.base.get_cover_bytes(
                item.media.cover_url,
            )
            if cover_bytes and (self.overwrite or not Path(item.cover_path).exists()):
                self._write_cover_file(
                    item.cover_path,
                    cover_bytes,
                )

        if (
            item.synced_lyrics_path
            and not self.no_synced_lyrics_file
            and item.media.lyrics
            and item.media.lyrics.synced
            and (self.overwrite or not Path(item.synced_lyrics_path).exists())
        ):
            self._write_synced_lyrics_file(
                item.synced_lyrics_path,
                item.media.lyrics.synced,
            )

    async def _final_processing(
        self,
        item: DownloadItem,
    ) -> None:
        if self.skip_processing:
            return

        if item.staged_path and item.final_path and Path(item.staged_path).exists():
            self._move_to_final_path(
                item.staged_path,
                item.final_path,
            )

    def _write_cover_file(self, cover_path: str, cover_bytes: bytes) -> None:
        logger.debug(f"Writing cover: {cover_path}")

        Path(cover_path).parent.mkdir(parents=True, exist_ok=True)
        with open(cover_path, "wb") as f:
            f.write(cover_bytes)

    def _write_synced_lyrics_file(self, synced_lyrics_path: str, lyrics: str) -> None:
        logger.debug(f"Writing synced lyrics: {synced_lyrics_path}")

        Path(synced_lyrics_path).parent.mkdir(parents=True, exist_ok=True)
        with open(synced_lyrics_path, "w", encoding="utf-8") as f:
            f.write(lyrics)

    def _move_to_final_path(self, staged_path: str, final_path: str) -> None:
        logger.debug(f'Moving "{staged_path}" to "{final_path}"')

        Path(final_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(staged_path, final_path)

    def finalize_playlists(self) -> None:
        """
        Write all collected playlists to disk.
        ✨ ALWAYS creates M3U8 files, even if:
        - Only skipped tracks (all files already exist)
        - Mix of downloaded and skipped tracks
        - Only newly downloaded tracks
        Call this after processing all tracks in a URL.
        """
        if self.save_playlist_file:
            if self.playlist_manager.playlists:
                logger.info("Creating/Updating M3U8 playlist files...")
                try:
                    self.playlist_manager.write_all_playlists()
                    stats = self.playlist_manager.get_stats()
                    if stats['total_playlists'] > 0:
                        logger.info(
                            f"✓ Finalized {stats['total_playlists']} M3U8 file(s) "
                            f"with {stats['total_tracks']} total track entries"
                        )
                except Exception as e:
                    logger.error(f"Error finalizing playlists: {e}")
                    raise
                finally:
                    # Clear for next URL
                    self.playlist_manager.clear()
            else:
                logger.debug("No playlists registered for finalization")

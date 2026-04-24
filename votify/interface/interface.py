import asyncio
import logging
from typing import AsyncGenerator, Callable

from InquirerPy import inquirer
from InquirerPy.base.control import Choice

from .audio import SpotifyAudioInterface
from .enums import AutoMediaOption
from .episode import SpotifyEpisodeInterface
from .episode_video import SpotifyEpisodeVideoInterface
from .exceptions import (
    VotifyMediaFlatFilterException,
    VotifyMediaNotFoundException,
    VotifyMediaUnstreamableException,
    VotifyUnsupportedMediaTypeException,
)
from .music_video import SpotifyMusicVideoInterface
from .song import SpotifySongInterface
from .types import SpotifyMedia

logger = logging.getLogger(__name__)


class SpotifyInterface:
    def __init__(
        self,
        base: SpotifyAudioInterface,
        song: SpotifySongInterface,
        episode: SpotifyEpisodeInterface,
        music_video: SpotifyMusicVideoInterface,
        episode_video: SpotifyEpisodeVideoInterface,
        prefer_video: bool = False,
        flat_filter: Callable = None,
    ) -> None:
        self.base = base
        self.song = song
        self.episode = episode
        self.music_video = music_video
        self.episode_video = episode_video
        self.prefer_video = prefer_video
        self.flat_filter = flat_filter

    async def _get_track_media(
        self,
        track_id: str,
        album_data: dict | None = None,
        album_items: list[dict] | None = None,
    ) -> SpotifyMedia | BaseException | None:
        track_response = await self.base.api.get_track(track_id)

        # Handle skipped tracks (when API returns None due to errors)
        if track_response is None:
            logger.warning(f"Track {track_id} was skipped by API, returning None")
            return None

        track_data = track_response["data"]["trackUnion"]

        if track_data["__typename"] != "Track":
            return VotifyMediaNotFoundException(track_id, track_data)

        if not track_data["playability"]["playable"]:
            return VotifyMediaUnstreamableException(track_id, track_data)

        if self.flat_filter:
            flat_filter_result = self.flat_filter(track_data)
            if asyncio.iscoroutine(flat_filter_result):
                flat_filter_result = await flat_filter_result

            if flat_filter_result:
                return VotifyMediaFlatFilterException(
                    track_id,
                    track_data,
                    flat_filter_result,
                )

        try:
            if (
                track_data["mediaType"] == "VIDEO"
                or self.prefer_video
                and track_data["associationsV3"]["videoAssociations"]["totalCount"]
            ):
                return await self.music_video.proccess_media(
                    **(
                        {
                            "track_data": track_data,
                            "album_data": album_data,
                        }
                        if track_data["mediaType"] == "VIDEO"
                        else {
                            "track_id": track_id,
                        }
                    ),
                )

            return await self.song.proccess_media(
                track_data=track_data,
                album_data=album_data,
                album_items=album_items,
            )
        except BaseException as e:
            return e

    async def _get_episode_media(
        self,
        episode_id: str,
        show_data: dict | None = None,
        show_items: list[dict] | None = None,
    ) -> SpotifyMedia | BaseException | None:
        episode_response = await self.base.api.get_episode(episode_id)

        # Handle skipped episodes (when API returns None due to errors)
        if episode_response is None:
            logger.warning(f"Episode {episode_id} was skipped by API, returning None")
            return None

        episode_data = episode_response["data"]["episodeUnionV2"]

        if episode_data["__typename"] != "Episode":
            return VotifyMediaNotFoundException(episode_id, episode_data)

        if not episode_data["playability"]["playable"]:
            return VotifyMediaUnstreamableException(episode_id, episode_data)

        if self.flat_filter:
            flat_filter_result = self.flat_filter(episode_data)
            if asyncio.iscoroutine(flat_filter_result):
                flat_filter_result = await flat_filter_result

            if flat_filter_result:
                return VotifyMediaFlatFilterException(
                    episode_id,
                    episode_data,
                    flat_filter_result,
                )

        try:
            if "VIDEO" in episode_data["mediaTypes"] and self.prefer_video:
                return await self.episode_video.proccess_media(
                    episode_data=episode_data,
                    show_data=show_data,
                    show_items=show_items,
                )

            return await self.episode.proccess_media(
                episode_data=episode_data,
                show_data=show_data,
                show_items=show_items,
            )
        except BaseException as e:
            return e

    async def _get_album_media(
        self,
        media_id: str,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        album_data, album_items = await self.base.get_album_data_cached(
            album_id=media_id
        )

        if album_data is None:
            logger.error(f"Album {media_id} data is None")
            yield VotifyMediaNotFoundException(media_id, {"error": "Album data is None"})
            return

        if album_data["__typename"] != "Album":
            yield VotifyMediaNotFoundException(media_id, album_data)
        else:
            for item in album_items:
                track_data = item["track"]
                track_id = track_data["uri"].split(":")[-1]

                media = await self._get_track_media(
                    track_id=track_id,
                    album_data=album_data,
                    album_items=album_items,
                )

                # Skip if track fetch failed
                if media is None:
                    logger.info(
                        f"Skipping track {track_id} in album - failed to fetch metadata"
                    )
                    continue

                yield media

    async def _get_show_media(
        self,
        media_id: str,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        show_data, show_items = await self.base.get_show_data_cached(show_id=media_id)

        if show_data is None:
            logger.error(f"Show {media_id} data is None")
            yield VotifyMediaNotFoundException(media_id, {"error": "Show data is None"})
            return

        if show_data["__typename"] != "Podcast":
            yield VotifyMediaNotFoundException(media_id, show_data)
        else:
            for item in show_items:
                episode_id = item["entity"]["_uri"].split(":")[-1]

                media = await self._get_episode_media(
                    episode_id=episode_id,
                    show_data=show_data,
                    show_items=show_items,
                )

                # Skip if episode fetch failed
                if media is None:
                    logger.info(
                        f"Skipping episode {episode_id} in show - failed to fetch metadata"
                    )
                    continue

                yield media

    async def _get_playlist_media(
        self,
        media_id: str,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        playlist_response = await self.base.api.get_playlist(media_id)

        # Handle skipped playlists
        if playlist_response is None:
            logger.error(f"Playlist {media_id} was skipped by API")
            yield VotifyMediaNotFoundException(media_id, {"error": "API returned None"})
            return

        playlist_data = playlist_response["data"]["playlistV2"]

        if playlist_data["__typename"] != "Playlist":
            yield VotifyMediaNotFoundException(media_id, playlist_data)
            return

        playlist_items = playlist_data["content"]["items"]

        # Fetch all playlist items (pagination)
        while len(playlist_items) < playlist_data["content"]["totalCount"]:
            playlist_response = await self.base.api.get_playlist(
                media_id,
                len(playlist_items),
            )

            # Handle paginated playlist fetch failure
            if playlist_response is None:
                logger.warning(
                    f"Pagination failed for playlist {media_id}, stopping at {len(playlist_items)} items"
                )
                break

            playlist_items.extend(
                playlist_response["data"]["playlistV2"]["content"]["items"]
            )

        # OPTIMIZATION: Batch database check for all tracks at once
        existing_tracks: dict[str, str] = {}
        if self.flat_filter and hasattr(self.flat_filter, "__self__"):
            track_ids: list[str] = []
            for item in playlist_items:
                track_data = item["itemV2"]["data"]
                track_id = track_data["uri"].split(":")[-1]
                track_ids.append(track_id)

            logger.info(f"Performing batch database check for {len(track_ids)} tracks...")
            database = self.flat_filter.__self__
            if hasattr(database, "get_batch"):
                existing_tracks = database.get_batch(track_ids)
                logger.info(
                    f"Found {len(existing_tracks)}/{len(track_ids)} tracks already downloaded"
                )

        # Process each track
        for index, item in enumerate(playlist_items, start=1):
            track_data = item["itemV2"]["data"]
            track_id = track_data["uri"].split(":")[-1]

            # Skip if already in database (using batch check results)
            if track_id in existing_tracks:
                logger.debug(
                    f"[{index}/{len(playlist_items)}] Track already exists, skipping API call"
                )

                # FIX: still create minimal SpotifyMedia so playlist_tags exist.
                minimal_media = SpotifyMedia(
                    media_id=track_id,
                    media_metadata=track_data,
                )
                minimal_media.playlist_metadata = playlist_data
                minimal_media.playlist_tags = self.base.get_playlist_tags(
                    playlist_data, index
                )

                exc = VotifyMediaFlatFilterException(
                    track_id,
                    track_data,
                    existing_tracks[track_id],
                )

                # Attach the minimal media and known file path (from DB)
                exc.media = minimal_media
                exc.file_path = existing_tracks[track_id]

                yield exc
                continue

            if track_data["__typename"] == "Track":
                media = await self._get_track_media(
                    track_id=track_id,
                )
            elif track_data["__typename"] == "Episode":
                media = await self._get_episode_media(
                    episode_id=track_id,
                )
            else:
                yield VotifyMediaNotFoundException(track_id, track_data)
                continue

            # Skip if track/episode fetch failed
            if media is None:
                logger.info(
                    f"Skipping item {index} in playlist - failed to fetch metadata"
                )
                continue

            media.playlist_metadata = playlist_data
            media.playlist_tags = self.base.get_playlist_tags(playlist_data, index)

            yield media

    async def _get_artist_media(
        self,
        media_id: str,
        auto_media_option: AutoMediaOption | None = None,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        if not auto_media_option:
            choices = [
                Choice(
                    name=" ".join(option.value.split("-")[1:]).capitalize(),
                    value=option,
                )
                for option in AutoMediaOption
                if option.value.startswith("artist-")
            ]
            artist_option = await inquirer.select(
                message="Select which media to download:",
                choices=choices,
            ).execute_async()
        else:
            artist_option = auto_media_option

        if artist_option in {
            AutoMediaOption.ARTIST_ALBUMS,
            AutoMediaOption.ARTIST_SINGLES,
            AutoMediaOption.ARTIST_COMPILATIONS,
        }:
            if artist_option == AutoMediaOption.ARTIST_ALBUMS:
                key = "albums"
            elif artist_option == AutoMediaOption.ARTIST_SINGLES:
                key = "singles"
            else:
                key = "compilations"

            async for media in self._get_artist_media_albums(
                media_id,
                key,
                bool(auto_media_option),
            ):
                yield media
        elif artist_option == AutoMediaOption.ARTIST_VIDEOS:
            async for media in self._get_artist_media_videos(
                media_id,
                bool(auto_media_option),
            ):
                yield media
        else:
            async for media in self._get_artist_top_tracks_media(
                media_id,
                bool(auto_media_option),
            ):
                yield media

    async def _get_artist_top_tracks_media(
        self,
        media_id: str,
        select_all: bool = False,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        artist_response = await self.base.api.get_artist_overview(media_id)

        # Handle skipped artist overview
        if artist_response is None:
            logger.error(f"Artist overview {media_id} was skipped by API")
            yield VotifyMediaNotFoundException(media_id, {"error": "API returned None"})
            return

        artist_data = artist_response["data"]["artistUnion"]

        if artist_data["__typename"] != "Artist":
            yield VotifyMediaNotFoundException(media_id, artist_data)
            return

        top_tracks_items = artist_data["discography"]["topTracks"]["items"]

        if not top_tracks_items:
            yield VotifyMediaNotFoundException(media_id, artist_data)
            return

        if select_all:
            selection = top_tracks_items
        else:
            choices = [
                Choice(
                    name=" | ".join(
                        [
                            track_item["track"]["name"],
                        ]
                    ),
                    value=track_item,
                )
                for track_item in top_tracks_items
            ]
            selection = await inquirer.select(
                message="Select which top tracks to download (Title):",
                choices=choices,
                multiselect=True,
            ).execute_async()

        for track_item in selection:
            track_id = track_item["track"]["id"]
            media = await self._get_track_media(
                track_id=track_id,
            )

            # Skip if track fetch failed
            if media is None:
                logger.info(
                    f"Skipping top track {track_id} - failed to fetch metadata"
                )
                continue

            yield media

    async def _get_artist_media_videos(
        self,
        media_id: str,
        select_all: bool = False,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        videos_response = await self.base.api.get_artist_videos(media_id)

        # Handle skipped artist videos
        if videos_response is None:
            logger.error(f"Artist videos {media_id} was skipped by API")
            yield VotifyMediaNotFoundException(media_id, {"error": "API returned None"})
            return

        videos_data = videos_response["data"]["artistUnion"]

        if videos_data["__typename"] != "Artist":
            yield VotifyMediaNotFoundException(media_id, videos_data)
            return

        related_items = videos_data["relatedMusicVideos"]["items"]
        unmapped_items = videos_data["unmappedMusicVideos"]["items"]
        related_total = videos_data["relatedMusicVideos"]["totalCount"]
        unmapped_total = videos_data["unmappedMusicVideos"]["totalCount"]

        while len(related_items) < related_total or len(unmapped_items) < unmapped_total:
            offset = max(len(related_items), len(unmapped_items))
            videos_response = await self.base.api.get_artist_videos(media_id, offset)

            # Handle paginated videos fetch failure
            if videos_response is None:
                logger.warning(
                    f"Pagination failed for artist videos {media_id}, stopping early"
                )
                break

            videos_data = videos_response["data"]["artistUnion"]
            if len(related_items) < related_total:
                related_items.extend(videos_data["relatedMusicVideos"]["items"])
            if len(unmapped_items) < unmapped_total:
                unmapped_items.extend(videos_data["unmappedMusicVideos"]["items"])

        video_items = related_items + unmapped_items

        if not video_items:
            yield VotifyMediaNotFoundException(media_id, videos_data)
            return

        if select_all:
            selection = video_items
        else:
            choices = [
                Choice(
                    name=" | ".join(
                        [
                            video_item["data"]["name"],
                        ]
                    ),
                    value=video_item,
                )
                for video_item in video_items
            ]
            selection = await inquirer.select(
                message="Select which videos to download (Title):",
                choices=choices,
                multiselect=True,
            ).execute_async()

        for video_item in selection:
            track_id = video_item["_uri"].split(":")[-1]
            media = await self._get_track_media(
                track_id=track_id,
            )

            # Skip if video fetch failed
            if media is None:
                logger.info(f"Skipping video {track_id} - failed to fetch metadata")
                continue

            yield media

    async def _get_artist_media_albums(
        self,
        media_id: str,
        key: str,
        select_all: bool = False,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        if key == "compilations":
            albums_response = await self.base.api.get_artist_compilations(media_id)
        elif key == "singles":
            albums_response = await self.base.api.get_artist_singles(media_id)
        else:
            albums_response = await self.base.api.get_artist_albums(media_id)

        # Handle skipped artist albums
        if albums_response is None:
            logger.error(f"Artist {key} {media_id} was skipped by API")
            yield VotifyMediaNotFoundException(media_id, {"error": "API returned None"})
            return

        albums_data = albums_response["data"]["artistUnion"]

        if albums_data["__typename"] != "Artist":
            yield VotifyMediaNotFoundException(media_id, albums_data)
            return

        album_items = albums_data["discography"][key]["items"]
        while len(album_items) < albums_data["discography"][key]["totalCount"]:
            if key == "compilations":
                albums_response = await self.base.api.get_artist_compilations(
                    media_id,
                    len(album_items),
                )
            elif key == "singles":
                albums_response = await self.base.api.get_artist_singles(
                    media_id,
                    len(album_items),
                )
            else:
                albums_response = await self.base.api.get_artist_albums(
                    media_id,
                    len(album_items),
                )

            # Handle paginated albums fetch failure
            if albums_response is None:
                logger.warning(
                    f"Pagination failed for artist {key} {media_id}, stopping early"
                )
                break

            album_items.extend(
                albums_response["data"]["artistUnion"]["discography"][key]["items"]
            )

        album_items_filtered = [
            release_item
            for album_item in album_items
            for release_item in album_item["releases"]["items"]
        ]

        if not album_items_filtered:
            yield VotifyMediaNotFoundException(media_id, albums_data)
            return

        if select_all:
            selection = album_items_filtered
        else:
            choices = [
                Choice(
                    name=" | ".join(
                        [
                            str(album_item["date"]["year"]),
                            f"{album_item['tracks']['totalCount']:03d}",
                            album_item["name"],
                        ]
                    ),
                    value=album_item,
                )
                for album_item in album_items_filtered
            ]
            selection = await inquirer.select(
                message="Select which albums to download (Year | Track Count | Title):",
                choices=choices,
                multiselect=True,
            ).execute_async()

        for album_item in selection:
            album_id = album_item["uri"].split(":")[-1]
            async for media in self._get_album_media(album_id):
                yield media

    async def _get_liked_tracks_media(
        self,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        offset = 0
        total = None

        while total is None or offset < total:
            liked_tracks_response = await self.base.api.get_library_tracks(offset)

            # Handle skipped library tracks
            if liked_tracks_response is None:
                logger.error(f"Library tracks at offset {offset} was skipped by API")
                break

            liked_tracks_data = liked_tracks_response["data"]["me"]["library"]["tracks"]

            if liked_tracks_data["__typename"] != "UserLibraryTrackPage":
                yield VotifyMediaNotFoundException("liked-tracks", liked_tracks_data)
                return

            total = liked_tracks_data["totalCount"]
            items = liked_tracks_data["items"]

            for item in items:
                track_data = item["track"]["data"]
                track_id = item["track"]["_uri"].split(":")[-1]

                if track_data["__typename"] == "Track":
                    media = await self._get_track_media(
                        track_id=track_id,
                    )
                elif track_data["__typename"] == "Episode":
                    media = await self._get_episode_media(
                        episode_id=track_id,
                    )
                else:
                    media = VotifyMediaNotFoundException(track_id, track_data)

                # Skip if track/episode fetch failed
                if media is None:
                    logger.info(
                        f"Skipping liked track {track_id} - failed to fetch metadata"
                    )
                    continue

                yield media

            offset += len(items)

    async def get_media(
        self,
        url: str | None = None,
        auto_media_option: AutoMediaOption | None = None,
    ) -> AsyncGenerator[SpotifyMedia | BaseException, None]:
        if auto_media_option == AutoMediaOption.LIKED_TRACKS:
            async for media in self._get_liked_tracks_media():
                yield media
            return

        url_info = self.base.parse_url_info(url)

        if not url_info or url_info.media_type in self.base.disallowed_media_types:
            yield VotifyUnsupportedMediaTypeException(
                getattr(
                    url_info,
                    "media_type",
                    "Null URL",
                ),
            )
        elif url_info.media_type == "track":
            media = await self._get_track_media(url_info.media_id)
            if media is not None:
                yield media
            else:
                logger.info(
                    f"Track {url_info.media_id} was skipped - failed to fetch metadata"
                )
        elif url_info.media_type == "episode":
            media = await self._get_episode_media(url_info.media_id)
            if media is not None:
                yield media
            else:
                logger.info(
                    f"Episode {url_info.media_id} was skipped - failed to fetch metadata"
                )
        elif url_info.media_type == "album":
            async for media in self._get_album_media(url_info.media_id):
                yield media
        elif url_info.media_type == "show":
            async for media in self._get_show_media(url_info.media_id):
                yield media
        elif url_info.media_type == "playlist":
            async for media in self._get_playlist_media(url_info.media_id):
                yield media
        elif url_info.media_type == "artist":
            async for media in self._get_artist_media(
                url_info.media_id,
                auto_media_option,
            ):
                yield media

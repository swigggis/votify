# Better Votify (Fork)

[![PyPI version](https://img.shields.io/pypi/v/votify?color=blue)](https://pypi.org/project/votify/)
[![Python versions](https://img.shields.io/pypi/pyversions/votify)](https://pypi.org/project/votify/)
[![License](https://img.shields.io/github/license/glomatico/votify)](https://github.com/glomatico/votify/blob/main/LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/votify)](https://pypi.org/project/votify/)

A command-line app for downloading songs, podcasts and videos from Spotify.

This repository is a **fork** of the original Votify by @glomatico:
- Upstream: https://github.com/glomatico/votify
- Fork: https://github.com/swigggis/better-votify/

## Fork goals / changes

This fork focuses on **robust playlist handling** for large collections:

- **Stable `.m3u8` creation for Spotify playlists**, even for very long playlists.
- **No “missing entries” for DB-skipped tracks**: when a track is skipped because it already exists (database/flat-filter), the fork still writes its **real file path** into the generated `.m3u8` playlist (as long as the DB has the file path).
- **Interruption-friendly** workflow: you can stop and resume runs without losing the ability to regenerate complete `.m3u8` files from existing downloads.

> Note: For `.m3u8` generation to include skipped tracks, you must use `database_path` so the downloader can map track IDs to existing file paths.

---

## ✨ Features

- 🎵 **Songs** - Download songs.
- 🎙️ **Podcasts** - Download podcasts.
- 🎬 **Videos** - Download podcast videos and music videos.
- 🎤 **Synced Lyrics** - Download synced lyrics in LRC format.
- 🧑‍🎤 **Artist Support** - Download an entire discography by providing the artist's URL.
- ⚙️ **Highly Customizable** - Extensive configuration options for advanced users.
- 🧾 **Playlist Files (M3U8)** - Generate playlist files for Spotify playlists (**including skipped/already-downloaded tracks** in this fork).

## 📋 Prerequisites

### Required

- **Python 3.10 or higher**
- **Spotify cookies** - Export your browser cookies in Netscape format while logged in at the Spotify homepage:
  - Firefox: [Export Cookies](https://addons.mozilla.org/addon/export-cookies-txt)
  - Chromium-based browsers: [Get cookies.txt LOCALLY](https://chromewebstore.google.com/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc)

> [!WARNING]
> - Some users have reported account action after using Spotify downloaders. Use at your own risk.

### Dependencies

Add tools to PATH or provide their paths via CLI/config. Required tools depend on session type and desired quality.

(Dependencies table is inherited from upstream; see below.)

---

## 📦 Installation

```bash
pip install votify[librespot]

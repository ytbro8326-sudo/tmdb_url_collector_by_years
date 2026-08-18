"""
primesrc_pipeline.py  –  Unified PrimeSrc pipeline
====================================================

Stage 1  (primesrcembed.py logic)
    Read tmdb_movie_input_list.txt  →  fetch /api/v1/s for every tmdb embed URL
    →  collect all server option keys  →  write output_stage1_api_urls_list_found.txt and output_stage1_api_urls_list_not_found.txt

Stage 2  (extract_primesrc_urls.py logic)
    Resolve keys from Stage 1  →  send every /api/v1/l?key=… to FlareSolverr
    →  extract stream URL from the JSON response
    →  on success: save tmdb_id to already_processed_urls_list.txt
                   remove that tmdb_id's error entries from errorsfaced.txt

Stage 3  –  GitHub sync
    Fetch movie_streaming_data.json (and movie_streaming_data-2.json, -3.json …)
    from the target GitHub repo via the Contents API.
    Merge new results in (upsert by tmdb_id, deduplicate sources).
    Auto-split: when a file reaches >= GITHUB_FILE_SIZE_LIMIT bytes,
    overflow entries are written to the next numbered file.
    Push every changed file back via a single authenticated PUT.

Extras
  - Single CLI entry point, no manual hand-off between scripts
  - Stage 1 uses plain urllib (no browser overhead)
  - --skip-stage1 / --skip-stage2 for incremental runs
  - Deduplication of keys before Stage 2 runs
  - already_processed_urls_list.txt tracks tmdb_ids (not raw API keys)
  - errorsfaced.txt auto-cleaned: resolved tmdb entries are removed
  - Graceful Ctrl-C at any stage

GitHub env vars required for Stage 3:
  GH_TOKEN   – personal access token (repo scope)
  GH_REPO    – owner/repo  (e.g. srtfile/movie-data)
  GH_BRANCH  – branch to push to (default: main)
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import sys
import time
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse
from urllib.request import Request, urlopen

warnings.filterwarnings("ignore", category=ResourceWarning)

# ═══════════════════════════════════════════════════════════════
# PATHS & TUNABLES
# ═══════════════════════════════════════════════════════════════

HERE                       = Path(__file__).parent
DEFAULT_INPUT_FILE         = HERE / "tmdb_movie_input_list.txt"
DEFAULT_HOLLY_BOLLY_INPUT  = HERE / "lastet_released_holly_bolly_movies_list.txt"
DEFAULT_API_LIST_FOUND     = HERE / "output_stage1_api_urls_list_found.txt"
DEFAULT_API_LIST_NOT_FOUND = HERE / "output_stage1_api_urls_list_not_found.txt"
DEFAULT_JSON_SUMMARY       = HERE / "movie_streaming_data.json"
DEFAULT_ERROR_LOG          = HERE / "errorsfaced.txt"
DEFAULT_PROCESSED_URLS     = HERE / "already_processed_urls_list.txt"

STAGE1_REQUEST_TIMEOUT = 20    # urllib timeout per /api/v1/s call
STAGE2_BATCH_SIZE      = 2     # concurrent requests per batch
STAGE2_RELOADS         = 3     # retry attempts per failed URL
STAGE2_FINAL_RETRIES   = 2     # extra full retry passes for still-failed keys
STAGE2_BATCH_DELAY     = 2.0   # seconds between batches
STAGE2_BAN_COOLDOWN    = 20.0  # extra wait after IP-ban / block error

TMDB_ID_RE = re.compile(r"^\d+$")

# ═══════════════════════════════════════════════════════════════
# GITHUB SYNC & SPLIT CONSTANTS
# ═══════════════════════════════════════════════════════════════

MAX_OUTPUT_FILE_SIZE    = 30 * 1024 * 1024   # 30 MB maximum per output file
GITHUB_FILE_SIZE_LIMIT  = MAX_OUTPUT_FILE_SIZE
GITHUB_BASE_FILENAME    = "movie_streaming_data"
ERROR_LOG_GH_FILENAME   = "errorsfaced.txt"
ERROR_LOG_GH_SIZE_LIMIT = MAX_OUTPUT_FILE_SIZE
GITHUB_API_ROOT         = "https://api.github.com"


def _split_part_path(base_path: Path, part_num: int) -> Path:
    """Return base.ext for part 1, or base-2.ext, base-3.ext, etc."""
    if part_num == 1:
        return base_path
    stem = base_path.stem
    suffix = base_path.suffix
    return base_path.parent / f"{stem}-{part_num}{suffix}"


def _write_split_text_lines(base_path: Path, lines: list[str], max_bytes: int = MAX_OUTPUT_FILE_SIZE) -> list[Path]:
    """Write lines split across base.txt, base-2.txt, base-3.txt ... when size exceeds max_bytes."""
    written_paths: list[Path] = []
    if not lines:
        base_path.write_text("", encoding="utf-8")
        return [base_path]

    part_num = 1
    current_lines: list[str] = []
    current_bytes = 0

    for line in lines:
        line_bytes = len((line + "\n").encode("utf-8"))
        if current_lines and current_bytes + line_bytes > max_bytes:
            p = _split_part_path(base_path, part_num)
            p.write_text("\n".join(current_lines) + "\n", encoding="utf-8")
            written_paths.append(p)
            part_num += 1
            current_lines = []
            current_bytes = 0

        current_lines.append(line)
        current_bytes += line_bytes

    if current_lines or not written_paths:
        p = _split_part_path(base_path, part_num)
        p.write_text("\n".join(current_lines) + ("\n" if current_lines else ""), encoding="utf-8")
        written_paths.append(p)

    return written_paths


def _append_split_text(base_path: Path, content: str, max_bytes: int = MAX_OUTPUT_FILE_SIZE) -> Path:
    """Find the latest part file of base_path and append if under max_bytes, else create next part."""
    part = 1
    target_path = base_path
    content_bytes = len(content.encode("utf-8"))
    while True:
        p = _split_part_path(base_path, part)
        if not p.exists():
            target_path = p
            break
        if p.stat().st_size + content_bytes <= max_bytes:
            target_path = p
            break
        part += 1
        target_path = p

    with open(target_path, "a", encoding="utf-8") as f:
        f.write(content)
    return target_path


# ═══════════════════════════════════════════════════════════════
# CONSOLE HELPERS
# ═══════════════════════════════════════════════════════════════

_RESET  = "\033[0m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"

def _c(text: str, colour: str) -> str:
    try:
        return colour + text + _RESET if sys.stdout.isatty() else text
    except Exception:
        return text

# ── Error/warning log buffer ─────────────────────────────────────
_ERROR_LOG_ENTRIES: list[str] = []

def _record_log_entry(level: str, msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    _ERROR_LOG_ENTRIES.append(f"[{ts}] [{level}] {msg}")

def log_info(msg: str) -> None: print(_c(f"[INFO]  {msg}", _CYAN))
def log_ok(msg: str)   -> None: print(_c(f"[OK]    {msg}", _GREEN))
def log_warn(msg: str) -> None: print(_c(f"[WARN]  {msg}", _YELLOW))
def log_err(msg: str)  -> None:
    print(_c(f"[ERR]   {msg}", _RED))
    _record_log_entry("ERR", msg)
def log_head(msg: str) -> None: print(_c(f"\n{'='*60}\n{msg}\n{'='*60}", _BOLD))


def _ensure_file_exists(path: Path | None, default_content: str = "") -> Path | None:
    """Ensure the file and its parent directories exist, creating if missing."""
    if path is None:
        return None
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(default_content, encoding="utf-8")
        log_info(f"Auto-created file: {path}")
    return path


def _format_error_log_block() -> str:
    """Build this run's timestamped WARN/ERR block. Empty string if nothing to log."""
    if not _ERROR_LOG_ENTRIES:
        return ""
    header = (
        f"\n{'='*70}\n"
        f"Pipeline run finished: {datetime.now(timezone.utc).isoformat()}\n"
        f"Total warnings/errors: {len(_ERROR_LOG_ENTRIES)}\n"
        f"{'='*70}\n"
    )
    return header + "\n".join(_ERROR_LOG_ENTRIES) + "\n"


def write_error_log(path: Path) -> None:
    """Append this run's collected entries to the local errorsfaced.txt (auto-splitting if > 30MB)."""
    block = _format_error_log_block()
    if not block:
        return
    try:
        target = _append_split_text(path, block, max_bytes=MAX_OUTPUT_FILE_SIZE)
        log_ok(f"Error/warning log appended → {target}  ({len(_ERROR_LOG_ENTRIES)} entries)")
    except Exception as exc:
        print(_c(f"[ERR]   Could not write error log to {path}: {exc}", _RED))


def clean_error_log_for_resolved_tmdb_ids(path: Path, resolved_tmdb_ids: set[str]) -> None:
    """
    Remove from errorsfaced.txt any lines that reference a tmdb_id that was
    successfully resolved this run. A line is considered resolved-related if it
    contains 'tmdb=<id>' for one of the resolved IDs, or contains one of the
    api/v1/l?key= URLs whose tmdb_id is now resolved.

    We match on the tmdb_id patterns embedded in the log lines:
      - key references like  [ 16/49] https://primesrc.me/api/v1/l?key=nwbmK
        are matched by the api_url→tmdb mapping we pass in.
    """
    if not resolved_tmdb_ids or not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        kept  = []
        removed = 0
        for line in lines:
            # Keep separator/header lines always
            if line.startswith("=") or line.startswith("\n") or "Pipeline run" in line or "Total warnings" in line:
                kept.append(line)
                continue
            drop = False
            for tmdb_id in resolved_tmdb_ids:
                if f"tmdb={tmdb_id}" in line:
                    drop = True
                    break
            if drop:
                removed += 1
            else:
                kept.append(line)
        if removed:
            path.write_text("".join(kept), encoding="utf-8")
            log_ok(f"Cleaned {removed} resolved error line(s) from {path}")
    except Exception as exc:
        log_warn(f"Could not clean error log: {exc}")


def clean_error_log_for_resolved_api_urls(path: Path, resolved_api_urls: set[str]) -> None:
    """
    Remove lines from errorsfaced.txt that mention any of the successfully-
    resolved API URLs (e.g. https://primesrc.me/api/v1/l?key=nwbmK).
    """
    if not resolved_api_urls or not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        kept  = []
        removed = 0
        for line in lines:
            if line.startswith("=") or line.startswith("\n") or "Pipeline run" in line or "Total warnings" in line:
                kept.append(line)
                continue
            drop = any(api_url in line for api_url in resolved_api_urls)
            if drop:
                removed += 1
            else:
                kept.append(line)
        if removed:
            path.write_text("".join(kept), encoding="utf-8")
            log_ok(f"Cleaned {removed} resolved API-URL error line(s) from {path}")
    except Exception as exc:
        log_warn(f"Could not clean error log for resolved API URLs: {exc}")


# ═══════════════════════════════════════════════════════════════
# STAGE 1  –  embed URLs → /api/v1/s → output_stage1_api_urls_list_found.txt
# ═══════════════════════════════════════════════════════════════

@dataclass
class ServerOption:
    server_name: str
    key: str
    api_url: str
    main_url: str
    host_id: int | None = None
    title: str = ""
    quality: str = ""
    audio_language: str = ""


# ═══════════════════════════════════════════════════════════════
# HOST PRIORITY CONFIGURATION
# 1st: voe.sx (host_id: 48)
# 2nd: bysejikaue / filemoon (host_id: 66)
# 3rd: luluvdoo (host_id: 68)
# 4th: savefiles (host_id: 69)
# 5th: dood (host_id: 42)
# 6th: streamta.site (host_id: 43)
# 7th: filenoons / filelions (host_id: 64)
# 8th: streamwish.to (host_id: 65)
# ═══════════════════════════════════════════════════════════════

HOST_PRIORITY_ORDER: list[tuple[int, set[str], str]] = [
    (48, {"voe", "voe.sx"}, "voe.sx (host_id: 48)"),
    (66, {"bysejikaue", "bysejikuar", "bysejikuar.com", "filemoon", "filemoon.sx"}, "bysejikaue (host_id: 66)"),
    (68, {"luluvdoo", "luluvdoo.com", "lulu"}, "luluvdoo (host_id: 68)"),
    (69, {"savefiles", "savefiles.com", "savefile"}, "savefiles (host_id: 69)"),
    (42, {"dood", "dood.watch", "doodstream", "ds2play"}, "dood (host_id: 42)"),
    (43, {"streamta", "streamta.site", "streamtape", "streamtape.com"}, "streamta.site (host_id: 43)"),
    (64, {"filenoons", "filelions", "filelions.to", "filenoon"}, "filenoons (host_id: 64)"),
    (65, {"streamwish", "streamwish.to"}, "streamwish.to (host_id: 65)"),
]

def get_server_priority(opt: ServerOption) -> int:
    """Return priority rank (0 is highest priority, 999 is lowest)."""
    if opt.host_id is not None:
        for rank, (target_id, _, _) in enumerate(HOST_PRIORITY_ORDER):
            if opt.host_id == target_id:
                return rank

    name_lower = (opt.server_name or "").lower()
    for rank, (_, keywords, _) in enumerate(HOST_PRIORITY_ORDER):
        if any(kw in name_lower for kw in keywords):
            return rank

    return 999

def get_server_priority_label(opt: ServerOption) -> str:
    rank = get_server_priority(opt)
    if rank < len(HOST_PRIORITY_ORDER):
        return f"Priority #{rank + 1}: {HOST_PRIORITY_ORDER[rank][2]}"
    return f"Fallback ({opt.server_name or 'server'})"


def _build_server_api_url(main_url: str) -> str:
    parsed = urlparse(main_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if parsed.path.startswith("/embed/movie"):
        params.setdefault("type", "movie")
    elif parsed.path.startswith("/embed/tv"):
        params.setdefault("type", "tv")
    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'primesrc.me'}"
    return f"{base}/api/v1/s?{urlencode(params)}"


def _fetch_json_http(url: str, referer: str) -> Any:
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, */*",
            "Referer": referer,
        },
    )
    with urlopen(req, timeout=STAGE1_REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return json.loads(resp.read().decode(charset, errors="replace"))


def _normalise_embed_url(raw: str, media_type: str = "movie") -> str:
    raw = raw.strip()
    if TMDB_ID_RE.fullmatch(raw):
        return f"https://primesrc.me/embed/{media_type}?tmdb={raw}"
    if raw.startswith("primesrc.me/"):
        return "https://" + raw
    if raw.startswith("/embed/"):
        return "https://primesrc.me" + raw
    return raw


def _extract_tmdb_id(url: str) -> str:
    """Extract the tmdb=<id> value from an embed or API URL. Returns '' if not found."""
    qs = dict(x.split("=", 1) for x in urlparse(url).query.split("&") if "=" in x)
    return qs.get("tmdb", "")


def _find_server_lists(obj: Any) -> list[dict[str, Any]]:
    lists: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        servers = obj.get("servers")
        if isinstance(servers, list) and servers:
            if any(
                "key" in item or "file_name" in item
                for item in servers
                if isinstance(item, dict)
            ):
                info = obj.get("info") if isinstance(obj.get("info"), dict) else {}
                lists.append({"servers": servers, "info": info})
        for v in obj.values():
            lists.extend(_find_server_lists(v))
    elif isinstance(obj, list):
        for item in obj:
            lists.extend(_find_server_lists(item))
    return lists


def _options_from_server_list(servers: list[dict], main_url: str) -> list[ServerOption]:
    options: list[ServerOption] = []
    for item in servers:
        key  = str(item.get("key")  or "").strip()
        name = str(item.get("name") or "").strip()
        if not key:
            continue
        raw_hid = item.get("host_id") or item.get("hostId") or item.get("server_id") or item.get("id")
        try:
            host_id = int(raw_hid) if raw_hid is not None else None
        except (ValueError, TypeError):
            host_id = None
        options.append(ServerOption(
            server_name    = name,
            key            = key,
            api_url        = f"https://primesrc.me/api/v1/l?key={quote(key, safe='')}",
            main_url       = main_url,
            host_id        = host_id,
            title          = str(item.get("file_name")      or "").strip(),
            quality        = str(item.get("quality")        or "").strip(),
            audio_language = str(item.get("audio_language") or "").strip(),
        ))
    return options


def stage1_fetch_api_keys(
    input_files: list[Path] | Path,
    processed_urls_file: Path,
    media_type: str = "movie",
    api_list_found_file: Path | None = None,
    api_list_not_found_file: Path | None = None,
) -> list[ServerOption]:
    log_head("STAGE 1  –  Fetch server keys from PrimeSrc /api/v1/s")

    input_paths = [input_files] if isinstance(input_files, Path) else list(input_files)
    for ip in input_paths:
        _ensure_file_exists(ip, "")
    _ensure_file_exists(processed_urls_file, "")
    _ensure_file_exists(api_list_found_file, "")
    _ensure_file_exists(api_list_not_found_file, "")

    raw_lines: list[str] = []
    seen_raw_lines: set[str] = set()
    for ip in input_paths:
        if ip.exists():
            for l in ip.read_text(encoding="utf-8").splitlines():
                l_str = l.strip()
                if l_str and not l_str.startswith("#") and l_str not in seen_raw_lines:
                    seen_raw_lines.add(l_str)
                    raw_lines.append(l_str)

    input_desc = ", ".join(p.name for p in input_paths)
    log_info(f"Input embed URLs : {len(raw_lines)}  (from {input_desc})")

    # ── Load already-processed tmdb_ids so whole movies are skipped ──
    # Supports both bare tmdb IDs ("218") and full embed URLs
    # ("https://primesrc.me/embed/movie?tmdb=218") in the same file.
    already_processed_tmdb: set[str] = set()
    if processed_urls_file.exists():
        for _line in processed_urls_file.read_text(encoding="utf-8").splitlines():
            _line = _line.strip()
            if not _line or _line.startswith("#"):
                continue
            if _line.startswith("http"):
                # Full embed URL — extract the tmdb= value
                _tid = _extract_tmdb_id(_line)
                if _tid:
                    already_processed_tmdb.add(_tid)
            elif TMDB_ID_RE.fullmatch(_line):
                # Legacy bare tmdb ID
                already_processed_tmdb.add(_line)
    if already_processed_tmdb:
        log_info(f"Already-processed tmdb_ids: {len(already_processed_tmdb)} — will skip in Stage 1")

    seen_urls: set[str] = set()
    embed_urls: list[str] = []
    skipped_stage1 = 0
    for raw in raw_lines:
        url = _normalise_embed_url(raw, media_type)
        tmdb_id = _extract_tmdb_id(url)
        if tmdb_id and tmdb_id in already_processed_tmdb:
            skipped_stage1 += 1
            continue
        if url not in seen_urls:
            seen_urls.add(url)
            embed_urls.append(url)

    if skipped_stage1:
        log_info(f"Skipped {skipped_stage1} already-processed tmdb_id(s) in Stage 1")

    all_options: list[ServerOption] = []
    errors: list[tuple[str, str]] = []
    found_lines: list[str] = []
    not_found_embed_urls: list[str] = []

    for idx, embed_url in enumerate(embed_urls, 1):
        label = f"  [{idx:>4}/{len(embed_urls)}]"
        api_url = _build_server_api_url(embed_url)
        try:
            obj = _fetch_json_http(api_url, embed_url)
            server_lists = _find_server_lists(obj)
            if not server_lists:
                log_warn(f"{label} no server list  {embed_url}")
                not_found_embed_urls.append(embed_url)
                continue
            movie_options: list[ServerOption] = []
            for sl in server_lists:
                opts = _options_from_server_list(sl.get("servers", []), embed_url)
                movie_options.extend(opts)
            if not movie_options:
                log_warn(f"{label} 0 keys found  {embed_url}")
                not_found_embed_urls.append(embed_url)
                continue

            total_keys = len(movie_options)
            unique_movie_keys = {opt.key for opt in movie_options}
            unique_count = len(unique_movie_keys)

            all_options.extend(movie_options)
            found_lines.append(f"{embed_url} {unique_count} keys of {total_keys}")
            log_ok(f"{label} {unique_count} keys of {total_keys}  {embed_url}")
        except Exception as exc:
            errors.append((embed_url, str(exc)))
            not_found_embed_urls.append(embed_url)
            log_err(f"{label} {exc}  {embed_url}")

    # Deduplicate by key value
    seen_keys: set[str] = set()
    unique_options: list[ServerOption] = []
    for opt in all_options:
        if opt.key not in seen_keys:
            seen_keys.add(opt.key)
            unique_options.append(opt)

    log_info(f"Total keys : {len(all_options)}  (unique: {len(unique_options)})")
    log_info(f"Errors     : {len(errors)}")

    if api_list_found_file:
        written = _write_split_text_lines(api_list_found_file, found_lines, max_bytes=MAX_OUTPUT_FILE_SIZE)
        for w in written:
            log_ok(f"Written found summaries ({len(found_lines)}) → {w}")

    if api_list_not_found_file:
        written = _write_split_text_lines(api_list_not_found_file, not_found_embed_urls, max_bytes=MAX_OUTPUT_FILE_SIZE)
        for w in written:
            log_ok(f"Written not-found embed URLs ({len(not_found_embed_urls)}) → {w}")

    if errors:
        log_warn("Failed embed URLs (stage 1):")
        for url, err in errors:
            log_warn(f"  {url}  → {err}")

    return unique_options


# ═══════════════════════════════════════════════════════════════
# STAGE 2  –  Resolve keys → FlareSolverr → stream URLs
# ═══════════════════════════════════════════════════════════════

FLARESOLVERR_DEFAULT_URL = "http://localhost:8191"
FLARESOLVERR_MAX_TIMEOUT = 45_000  # ms

_print_lock: asyncio.Lock | None = None


async def safe_print(*a: Any, **kw: Any) -> None:
    async with _print_lock:  # type: ignore[union-attr]
        print(*a, **kw)


def extract_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty page content")
    if text[0] in "{[":
        return json.loads(text)
    s = text.find("{")
    e = text.rfind("}") + 1
    if s == -1 or e <= s:
        raise ValueError("No JSON object found in page")
    return json.loads(text[s:e])


def get_play_url(data: Any) -> str | None:
    if isinstance(data, dict):
        for key in ("link", "url", "file", "src", "stream"):
            v = data.get(key)
            if isinstance(v, str) and v.startswith(("http://", "https://")):
                return v
        for key in ("sources", "tracks", "streams"):
            items = data.get(key)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, str) and item.startswith(("http://", "https://")):
                        return item
                    if isinstance(item, dict):
                        nested = get_play_url(item)
                        if nested:
                            return nested
    elif isinstance(data, list):
        for item in data:
            nested = get_play_url(item)
            if nested:
                return nested
    return None


def _flaresolverr_url(args: argparse.Namespace) -> str:
    return (
        os.environ.get("FLARESOLVERR_URL")
        or getattr(args, "flaresolverr_url", None)
        or FLARESOLVERR_DEFAULT_URL
    ).rstrip("/")


def _fs_post(base_url: str, payload: dict[str, Any], http_timeout: int = 120) -> dict[str, Any]:
    import urllib.error
    data = json.dumps(payload).encode("utf-8")
    req  = Request(
        f"{base_url}/v1",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=http_timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
            fs_resp = json.loads(body)
            return {
                "status": "error",
                "message": fs_resp.get("message", body[:300]),
                "_http_status": exc.code,
            }
        except Exception:
            raise ConnectionError(
                f"FlareSolverr at {base_url}/v1 returned HTTP {exc.code}: {exc.reason}"
            ) from exc
    except urllib.error.URLError as exc:
        raise ConnectionError(
            f"Cannot reach FlareSolverr at {base_url}/v1 — is it running?  ({exc})"
        ) from exc


async def _fs_create_session(base_url: str, session_id: str) -> None:
    loop = asyncio.get_running_loop()
    resp = await loop.run_in_executor(
        None,
        lambda: _fs_post(base_url, {"cmd": "sessions.create", "session": session_id}),
    )
    if resp.get("status") not in ("ok", "warning"):
        log_warn(f"FlareSolverr session.create status: {resp.get('status')} — {resp.get('message')}")
    else:
        log_ok(f"FlareSolverr session created: {session_id}")


async def _fs_destroy_session(base_url: str, session_id: str) -> None:
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: _fs_post(base_url, {"cmd": "sessions.destroy", "session": session_id}),
        )
        log_info(f"FlareSolverr session destroyed: {session_id}")
    except Exception:
        pass


def _check_flaresolverr_health(base_url: str) -> bool:
    try:
        with urlopen(f"{base_url}/health", timeout=5) as resp:
            body = json.loads(resp.read())
            return body.get("status") == "ok"
    except Exception:
        return False


def _parse_flaresolverr_response(resp: dict[str, Any]) -> Any:
    solution  = resp.get("solution", {})
    body_html = solution.get("response", "")
    body_text = body_html
    m = re.search(r"<body[^>]*>(.*?)</body>", body_html, re.S | re.I)
    if m:
        body_text = re.sub(r"<[^>]+>", "", m.group(1))
    return extract_json(body_text)


async def _resolve_one_flaresolverr_core(
    base_url: str,
    session_id: str,
    api_url: str,
    timeout_ms: int,
    reloads: int,
    index: int,
    total: int,
    sub_label: str = "",
    known_media: dict[str, set[str]] | None = None,
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    label = f"[{index:>3}/{total}]{sub_label}"
    key_session_id = f"{session_id}_{index}_{int(time.time() * 1000) % 100000}"

    await loop.run_in_executor(None, lambda: _fs_post(base_url, {
        "cmd": "sessions.create", "session": key_session_id
    }))
    last_error: str | None = None

    try:
        for attempt in range(reloads + 1):
            if attempt:
                delay = 1.5 * (2 ** (attempt - 1))
                if last_error and re.search(r"banned|blocked", last_error, re.I):
                    delay = max(delay, 8.0)
                await safe_print(f"{label} ↻ FlareSolverr retry {attempt}/{reloads} (waiting {delay:.1f}s)")
                await asyncio.sleep(delay)

            try:
                fs_resp = await loop.run_in_executor(
                    None,
                    lambda: _fs_post(base_url, {
                        "cmd":        "request.get",
                        "url":        api_url,
                        "maxTimeout": timeout_ms,
                        "session":    key_session_id,
                    }),
                )

                if fs_resp.get("status") != "ok":
                    last_error = (
                        f"FlareSolverr error: {fs_resp.get('message', '')}"
                        + (f" (HTTP {fs_resp.get('_http_status')})" if "_http_status" in fs_resp else "")
                    )
                    continue

                data     = _parse_flaresolverr_response(fs_resp)
                play_url = get_play_url(data)

                if play_url:
                    return {
                        "index":         index,
                        "api_url":       api_url,
                        "data":          data,
                        "extracted_url": play_url,
                        "method":        "flaresolverr",
                    }

                if isinstance(data, dict):
                    for candidate_key in ("url", "link", "redirect", "location"):
                        candidate = data.get(candidate_key, "")
                        if isinstance(candidate, str) and candidate.startswith("http"):
                            return {
                                "index":         index,
                                "api_url":       api_url,
                                "data":          data,
                                "extracted_url": candidate,
                                "method":        "flaresolverr",
                            }

                last_error = f"no play URL in FS response: {str(data)[:120]}"

            except Exception as exc:
                last_error = str(exc)

        return {
            "index":         index,
            "api_url":       api_url,
            "error":         last_error or "failed",
            "extracted_url": None,
        }
    finally:
        try:
            await loop.run_in_executor(None, lambda: _fs_post(base_url, {
                "cmd": "sessions.destroy", "session": key_session_id
            }))
        except Exception:
            pass


async def _resolve_movie_options(
    movie_idx: int,
    total_movies: int,
    main_url: str,
    options: list[ServerOption],
    base_url: str,
    session_id: str,
    timeout_ms: int,
    reloads: int,
    sem: asyncio.Semaphore,
    known_media: dict[str, set[str]] | None = None,
) -> tuple[ServerOption | None, dict[str, Any] | None, list[dict[str, Any]]]:
    """
    Tries server options for a single movie in prioritized order:
    1st: voe.sx (host_id: 48)
    2nd: bysejikaue / filemoon (host_id: 66)
    3rd: luluvdoo (host_id: 68)
    4th: savefiles (host_id: 69)
    5th: dood (host_id: 42)
    6th: streamta.site (host_id: 43)
    7th: filenoons / filelions (host_id: 64)
    8th: streamwish.to (host_id: 65)

    Stops immediately upon finding the first working stream.
    """
    sorted_opts = sorted(options, key=get_server_priority)
    tmdb_id = _extract_tmdb_id(main_url)
    movie_label = f"Movie [{movie_idx:>3}/{total_movies}] (tmdb={tmdb_id})"
    all_attempts: list[dict[str, Any]] = []

    async with sem:
        for opt_idx, opt in enumerate(sorted_opts, 1):
            p_label = get_server_priority_label(opt)
            await safe_print(f"{movie_label} -> Trying {p_label} (opt {opt_idx}/{len(sorted_opts)}): {opt.api_url}")

            res = await _resolve_one_flaresolverr_core(
                base_url=base_url,
                session_id=session_id,
                api_url=opt.api_url,
                timeout_ms=timeout_ms,
                reloads=reloads,
                index=movie_idx,
                total=total_movies,
                sub_label=f"[{opt_idx}/{len(sorted_opts)}]",
                known_media=known_media,
            )
            all_attempts.append(res)

            if res.get("extracted_url"):
                stream_url = res["extracted_url"]
                host_info = ""
                if isinstance(res.get("data"), dict):
                    h = res["data"].get("host")
                    hid = res["data"].get("host_id")
                    if h or hid:
                        host_info = f" [host: {h}, host_id: {hid}]"
                await safe_print(f"{movie_label} -> SUCCESS with {p_label}{host_info}: {stream_url}")
                return opt, res, all_attempts
            else:
                err = res.get("error", "No URL found")
                if opt_idx < len(sorted_opts):
                    next_p_label = get_server_priority_label(sorted_opts[opt_idx])
                    await safe_print(f"{movie_label} -> Failed on {p_label} ({err}). Shifting to next: {next_p_label}")
                else:
                    await safe_print(f"{movie_label} -> Failed on {p_label} ({err}). No more server options for this movie.")

    return None, None, all_attempts


def _load_known_media_urls(json_path: Path) -> set[str]:
    """
    Load all host-N / url-N values from local movie_streaming_data*.json.
    Returns a set of media URLs that are already recorded, so Stage 2
    can suppress errors for API keys whose media is already known.
    """
    known: set[str] = set()
    part = 1
    while True:
        p = _split_part_path(json_path, part)
        if not p.exists():
            break
        try:
            records = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(records, list):
                for rec in records:
                    if not isinstance(rec, dict):
                        continue
                    n = 1
                    while True:
                        h = rec.get(f"host-{n}")
                        u = rec.get(f"url-{n}")
                        if not h and not u:
                            break
                        for val in (h, u):
                            if isinstance(val, str) and val.startswith("http"):
                                known.add(val)
                        n += 1
        except Exception as exc:
            log_warn(f"Could not load known media URLs from {p}: {exc}")
        part += 1
    return known


async def stage2_extract_stream_urls(
    stage1_options: list[ServerOption],
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    log_head("STAGE 2  –  Prioritized stream extraction via FlareSolverr\n(voe.sx [48] -> bysejikaue [66] -> luluvdoo [68] -> savefiles [69] -> dood [42] -> streamta [43] -> filenoons [64] -> streamwish [65])")

    global _print_lock
    _print_lock = asyncio.Lock()

    if not stage1_options:
        log_warn("No API keys to resolve in Stage 2.")
        return []

    # Group options by movie (main_url)
    movie_to_options: dict[str, list[ServerOption]] = defaultdict(list)
    for opt in stage1_options:
        movie_to_options[opt.main_url].append(opt)

    total_movies = len(movie_to_options)
    log_info(f"Total movies to resolve : {total_movies}")
    log_info(f"Total available server keys across movies : {len(stage1_options)}")
    log_info("Priority order : 1.voe.sx(48) -> 2.bysejikaue(66) -> 3.luluvdoo(68) -> 4.savefiles(69) -> 5.dood(42) -> 6.streamta(43) -> 7.filenoons(64) -> 8.streamwish(65)")

    # Load already-known media URLs
    json_out_path: Path = getattr(args, "json_out", DEFAULT_JSON_SUMMARY)
    _known_media_urls: set[str] = _load_known_media_urls(json_out_path)
    if _known_media_urls:
        log_info(f"Known media URLs loaded from JSON: {len(_known_media_urls)}")

    base_url   = _flaresolverr_url(args)
    timeout_ms = getattr(args, "fs_timeout_ms", FLARESOLVERR_MAX_TIMEOUT)
    session_id = f"primesrc_{int(time.time())}"

    log_info(f"FlareSolverr URL    : {base_url}")
    log_info(f"Concurrent movies   : {args.batch_size}")
    log_info(f"Reloads per host    : {args.reloads}")
    log_info(f"Solver timeout      : {timeout_ms} ms")

    log_info("Checking FlareSolverr health…")
    if not _check_flaresolverr_health(base_url):
        log_err(
            f"FlareSolverr is not reachable at {base_url}\n"
            "  Start it with Docker:\n"
            "    docker run -d -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest"
        )
        raise ConnectionError("FlareSolverr not reachable")
    log_ok("FlareSolverr is healthy")

    t_start = time.monotonic()
    sem = asyncio.Semaphore(args.batch_size)

    # Process movies concurrently with semaphore
    tasks = [
        _resolve_movie_options(
            movie_idx=idx,
            total_movies=total_movies,
            main_url=main_url,
            options=opts,
            base_url=base_url,
            session_id=session_id,
            timeout_ms=timeout_ms,
            reloads=args.reloads,
            sem=sem,
        )
        for idx, (main_url, opts) in enumerate(movie_to_options.items(), 1)
    ]

    movie_outcomes = await asyncio.gather(*tasks)

    results: list[dict[str, Any]] = []
    fully_resolved_tmdb: set[str] = set()
    succeeded_api_urls: set[str] = set()
    failed_movies: list[str] = []

    for (succ_opt, succ_res, attempts) in movie_outcomes:
        results.extend(attempts)
        if succ_opt and succ_res and succ_res.get("extracted_url"):
            succeeded_api_urls.add(succ_opt.api_url)
            tid = _extract_tmdb_id(succ_opt.main_url)
            if tid:
                fully_resolved_tmdb.add(tid)
        else:
            if attempts:
                failed_movies.append(attempts[0].get("api_url", ""))

    processed_urls_file: Path = getattr(args, "processed_urls", DEFAULT_PROCESSED_URLS)
    error_log_file: Path      = getattr(args, "error_log", DEFAULT_ERROR_LOG)

    # Save newly resolved tmdb IDs
    existing_processed: set[str] = set()
    if processed_urls_file.exists():
        for _line in processed_urls_file.read_text(encoding="utf-8").splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#"):
                existing_processed.add(_line)

    new_tmdb_ids = fully_resolved_tmdb - existing_processed
    if new_tmdb_ids:
        tmdb_to_embed_url: dict[str, str] = {}
        for _opt in stage1_options:
            _tid = _extract_tmdb_id(_opt.main_url)
            if _tid and _tid not in tmdb_to_embed_url:
                tmdb_to_embed_url[_tid] = _opt.main_url

        lines_to_append = ""
        for tmdb_id in sorted(new_tmdb_ids, key=int):
            embed_url = tmdb_to_embed_url.get(
                tmdb_id,
                f"https://primesrc.me/embed/movie?tmdb={tmdb_id}",
            )
            lines_to_append += embed_url + "\n"

        target_pf = _append_split_text(processed_urls_file, lines_to_append, max_bytes=MAX_OUTPUT_FILE_SIZE)
        log_ok(f"Saved {len(new_tmdb_ids)} fully-resolved tmdb_id(s) → {target_pf}: {sorted(new_tmdb_ids)}")

    # Clean errorsfaced.txt
    if fully_resolved_tmdb:
        clean_error_log_for_resolved_tmdb_ids(error_log_file, fully_resolved_tmdb)
    if succeeded_api_urls:
        clean_error_log_for_resolved_api_urls(error_log_file, succeeded_api_urls)

    global _ERROR_LOG_ENTRIES
    _ERROR_LOG_ENTRIES = [
        e for e in _ERROR_LOG_ENTRIES
        if not any(f"tmdb={tid}" in e for tid in fully_resolved_tmdb)
        and not any(u in e for u in succeeded_api_urls)
    ]

    elapsed = time.monotonic() - t_start
    log_head(f"STAGE 2 RESULTS  ({elapsed:.1f}s total)")
    log_ok(f"Resolved movies : {len(fully_resolved_tmdb)} / {total_movies}")
    log_info(f"Extracted stream URLs : {len(succeeded_api_urls)}")
    if failed_movies:
        log_warn(f"Movies without any working stream : {len(failed_movies)}")

    return results


# ═══════════════════════════════════════════════════════════════
# TMDB TITLE LOOKUP
# ═══════════════════════════════════════════════════════════════

TMDB_API_KEY = "6fad3f86b8452ee232deb7977d7dcf58"

def _tmdb_request(path: str) -> dict:
    base = "https://api.themoviedb.org/3"
    sep  = "&" if "?" in path else "?"
    url  = f"{base}{path}{sep}language=en-US"
    if TMDB_API_KEY:
        url += f"&api_key={TMDB_API_KEY}"
    req = Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    })
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _fetch_tmdb_info(tmdb_id: str) -> tuple[str, str]:
    title   = ""
    imdb_id = None
    try:
        data    = _tmdb_request(f"/movie/{tmdb_id}")
        title   = data.get("title") or data.get("original_title") or ""
        imdb_id = data.get("imdb_id") or None
        if not imdb_id:
            ext     = _tmdb_request(f"/movie/{tmdb_id}/external_ids")
            imdb_id = ext.get("imdb_id") or None
    except Exception as exc:
        log_warn(f"TMDB info fetch failed for tmdb={tmdb_id}: {exc}")
    return title, imdb_id


def fetch_tmdb_now_playing_and_top_rated(
    target_file: Path,
    existing_input_files: list[Path] | Path | None = None,
    processed_urls_file: Path | None = None,
    limit: int = 50,
) -> list[str]:
    """
    Fetch movies from TMDB Now Playing (/movie/now_playing) and Top Rated (/movie/top_rated) combined
    up to limit total (default 50).
    For Now Playing: strictly filters to movies released between current today date and previous 2 months (last 60 days).
    Excludes any movies already in target_file, existing_input_files, or processed_urls_file.
    Appends newly discovered movie embed URLs (https://primesrc.me/embed/movie?tmdb=<id>) to target_file.
    Returns the list of newly added embed URLs.
    """
    _ensure_file_exists(target_file, "")

    today = datetime.now(timezone.utc).date()
    two_months_ago = today - timedelta(days=60)

    def _is_within_last_two_months(date_str: str) -> bool:
        if not date_str:
            return False
        try:
            d = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
            return two_months_ago <= d <= today
        except Exception:
            return False

    known_tmdb_ids: set[str] = set()
    files_to_check: list[Path] = []
    if isinstance(existing_input_files, list):
        files_to_check.extend(existing_input_files)
    elif existing_input_files:
        files_to_check.append(existing_input_files)
    if processed_urls_file:
        files_to_check.append(processed_urls_file)
    if target_file:
        files_to_check.append(target_file)

    for f in files_to_check:
        if f and f.exists():
            for line in f.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    tid = _extract_tmdb_id(line)
                    if not tid and TMDB_ID_RE.fullmatch(line):
                        tid = line
                    if tid:
                        known_tmdb_ids.add(tid)

    log_info(f"Existing known TMDB IDs to exclude: {len(known_tmdb_ids)}")
    log_info(f"TMDB Now Playing date filter window: {two_months_ago} to {today} (last 2 months)")

    def _fetch_from_endpoint(endpoint: str, target_count: int, filter_recent_date: bool = False) -> list[str]:
        added: list[str] = []
        page = 1
        max_pages = 25
        while len(added) < target_count and page <= max_pages:
            sep = "&" if "?" in endpoint else "?"
            path = f"{endpoint}{sep}page={page}"
            try:
                data = _tmdb_request(path)
                results = data.get("results", [])
                if not results:
                    break
                for m in results:
                    mid = str(m.get("id", ""))
                    if not mid:
                        continue

                    # Filter by release date (current date to previous 2 months)
                    if filter_recent_date:
                        rel_date = str(m.get("release_date") or "").strip()
                        if not _is_within_last_two_months(rel_date):
                            continue

                    if mid not in known_tmdb_ids:
                        known_tmdb_ids.add(mid)
                        embed_url = f"https://primesrc.me/embed/movie?tmdb={mid}"
                        added.append(embed_url)
                        if len(added) >= target_count:
                            break
                page += 1
            except Exception as exc:
                log_warn(f"Failed fetching TMDB {endpoint} page={page}: {exc}")
                break
        return added

    # 1. Query Now Playing first (strictly last 2 months)
    log_info(f"Fetching from TMDB Now Playing (target: up to {limit}, release window: {two_months_ago} to {today})…")
    np_urls = _fetch_from_endpoint("/movie/now_playing", limit, filter_recent_date=True)
    log_ok(f"Found {len(np_urls)} new Now Playing movie(s) released within last 2 months")

    # 2. Fill remainder from Top Rated
    remaining = limit - len(np_urls)
    tr_urls: list[str] = []
    if remaining > 0:
        log_info(f"Filling remaining ({remaining}) from TMDB Top Rated…")
        tr_urls = _fetch_from_endpoint("/movie/top_rated", remaining, filter_recent_date=False)
        log_ok(f"Found {len(tr_urls)} new Top Rated movie(s)")

    new_urls = np_urls + tr_urls
    if new_urls:
        lines_to_append = "\n".join(new_urls) + "\n"
        target_file_written = _append_split_text(target_file, lines_to_append, max_bytes=MAX_OUTPUT_FILE_SIZE)
        log_ok(f"Stored {len(new_urls)} combined (now-playing: {len(np_urls)}, top-rated: {len(tr_urls)}) movie(s) → {target_file_written}")
    else:
        log_info("No new Now Playing or Top Rated movies found that aren't already recorded.")

    return new_urls


# ═══════════════════════════════════════════════════════════════
# SUMMARY WRITER
# ═══════════════════════════════════════════════════════════════

def _parse_entry_from_record(e: dict[str, Any]) -> tuple[int, str, str, str, list[dict[str, str]]]:
    """Parse tmdb_int, imdb_id, title, extracted_at, sources from either old or new JSON format."""
    tmdb_int = 0
    imdb_id = ""
    if "tmdb/imdb" in e:
        val = str(e["tmdb/imdb"])
        parts = val.split("/", 1)
        try:
            tmdb_int = int(parts[0].strip())
        except ValueError:
            tmdb_int = 0
        if len(parts) > 1 and parts[1].strip():
            imdb_id = parts[1].strip()
    else:
        tmdb_int = int(e.get("tmdb_id", 0) or 0)
        imdb_id = str(e.get("imdb_id") or "")

    title = e.get("title", "")
    extracted_at = e.get("extracted_at", "")

    sources: list[dict[str, str]] = []
    n = 1
    while True:
        h = e.get(f"host-{n}")
        u = e.get(f"url-{n}")
        if not h and not u:
            break
        url = u if (isinstance(u, str) and u.startswith("http")) else (h if (isinstance(h, str) and h.startswith("http")) else "")
        if url:
            sources.append({"url": url, "key": e.get(f"key-{n}", url)})
        n += 1

    return tmdb_int, imdb_id, title, extracted_at, sources


def _format_summary_json(records: list[dict[str, Any]]) -> str:
    import re as _re

    def _jv(v: Any) -> str:
        return json.dumps(v, ensure_ascii=False)

    lines: list[str] = ["["]
    for rec_idx, rec in enumerate(records):
        lines.append("  {")
        header_keys = ["serial", "title", "tmdb/imdb", "extracted_at"]
        n_hosts = sum(1 for k in rec if _re.fullmatch(r"host-\d+", k))

        all_field_lines: list[str] = []

        for hk in header_keys:
            if hk in rec:
                all_field_lines.append(f'    {_jv(hk)}: {_jv(rec[hk])}')

        for n in range(1, n_hosts + 1):
            hkey = f"host-{n}"
            if hkey in rec:
                all_field_lines.append(f'    {_jv(hkey)}: {_jv(rec[hkey])}')

        is_last_rec = rec_idx == len(records) - 1
        for fi, fl in enumerate(all_field_lines):
            is_last_field = fi == len(all_field_lines) - 1
            if is_last_field:
                lines.append(fl)
            else:
                lines.append(fl + ",")

        if is_last_rec:
            lines.append("  }")
        else:
            lines.append("  },")

    lines.append("]")
    return "\n".join(lines) + "\n"


def _write_summary(
    stage1_options: list[ServerOption],
    stage2_results: list[dict[str, Any]],
    json_path: Path,
) -> None:
    link_map = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}

    new_groups_raw: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups_raw[tmdb][opt.api_url] = {"host": urlparse(stream_url).netloc, "url": stream_url, "key": opt.api_url}
    new_groups: dict[str, list[dict[str, Any]]] = {
        tmdb: list(entry_map.values()) for tmdb, entry_map in new_groups_raw.items()
    }

    existing: list[dict[str, Any]] = []
    part = 1
    while True:
        p = _split_part_path(json_path, part)
        if not p.exists():
            break
        try:
            part_records = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(part_records, list):
                existing.extend(part_records)
                log_info(f"Loaded {len(part_records)} existing entries from {p}")
        except Exception as exc:
            log_warn(f"Could not load existing JSON from {p} ({exc})")
        part += 1

    index: dict[int, dict[str, Any]] = {}
    for e in existing:
        tmdb_int, imdb_id, title, extracted_at, sources = _parse_entry_from_record(e)
        if not tmdb_int:
            continue
        index[tmdb_int] = {
            "tmdb_id":      tmdb_int,
            "imdb_id":      imdb_id,
            "title":        title,
            "extracted_at": extracted_at,
            "_sources":     sources,
        }

    extracted_at     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmdb_meta_cache: dict[int, tuple[str, Any]] = {}

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)
        if tmdb_int in index:
            entry         = index[tmdb_int]
            existing_keys = {s["key"] for s in entry["_sources"]}
            added         = [s for s in new_sources if s["key"] not in existing_keys]
            entry["_sources"].extend(added)
            entry["extracted_at"] = extracted_at
            log_info(f"  tmdb={tmdb_int} — merged {len(added)} new source(s)")
        else:
            if tmdb_int not in tmdb_meta_cache:
                log_info(f"  tmdb={tmdb_int} — fetching title + imdb_id…")
                title, imdb_id = _fetch_tmdb_info(tmdb_str)
                tmdb_meta_cache[tmdb_int] = (title, imdb_id)
                log_ok(f"  tmdb={tmdb_int} — '{title}'  imdb={imdb_id}")
            else:
                title, imdb_id = tmdb_meta_cache[tmdb_int]
            index[tmdb_int] = {
                "tmdb_id":      tmdb_int,
                "imdb_id":      imdb_id,
                "title":        title,
                "extracted_at": extracted_at,
                "_sources":     list(new_sources),
            }
            log_ok(f"  tmdb={tmdb_int} — '{title}'  sources: {len(new_sources)}")

    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    output: list[dict[str, Any]] = []
    for e in sorted_entries:
        tmdb_val = str(e["tmdb_id"])
        imdb_val = str(e.get("imdb_id") or "")
        tmdb_imdb_val = f"{tmdb_val}/{imdb_val}" if imdb_val else f"{tmdb_val}/"

        row: dict[str, Any] = {
            "serial":       e["serial"],
            "title":        e.get("title", ""),
            "tmdb/imdb":    tmdb_imdb_val,
            "extracted_at": e["extracted_at"],
        }
        for n, src in enumerate(e["_sources"], 1):
            row[f"host-{n}"] = src["url"]
        output.append(row)

    chunks = _gh_split_records(output)
    for i, chunk_bytes in enumerate(chunks, 1):
        target_json = _split_part_path(json_path, i)
        target_json.write_bytes(chunk_bytes)
        log_ok(f"Pretty JSON ({len(chunk_bytes):,} B) → {target_json}")

    total_sources = sum(sum(1 for k in row if k.startswith("host-")) for row in output)
    log_info(f"Movies : {len(output)}   Sources : {total_sources}   Split into {len(chunks)} file(s)")


# ═══════════════════════════════════════════════════════════════
# STAGE 3  –  GITHUB SYNC
# ═══════════════════════════════════════════════════════════════

def _gh_filename(n: int) -> str:
    if n == 1:
        return f"{GITHUB_BASE_FILENAME}.json"
    return f"{GITHUB_BASE_FILENAME}-{n}.json"


def _gh_api_request(
    method: str,
    path: str,
    token: str,
    payload: dict[str, Any] | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    import urllib.error
    url  = GITHUB_API_ROOT + path
    data = json.dumps(payload).encode("utf-8") if payload else None
    req  = Request(
        url,
        data=data,
        headers={
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type":         "application/json",
            "User-Agent":           "primesrc-pipeline/1.0",
        },
        method=method,
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {path} → HTTP {exc.code}: {body[:400]}") from exc


def _gh_get_file(
    token: str,
    repo: str,
    path: str,
    branch: str,
) -> tuple[list[dict[str, Any]], str | None]:
    api_path = f"/repos/{repo}/contents/{path}?ref={branch}"
    try:
        meta = _gh_api_request("GET", api_path, token)
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return [], None
        raise

    raw_b64 = meta.get("content", "").replace("\n", "")
    sha     = meta.get("sha")
    if not raw_b64:
        return [], sha
    try:
        raw_bytes = base64.b64decode(raw_b64)
        records   = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(records, list):
            records = []
        log_info(f"  GitHub ← {path}: {len(records)} entries (sha={sha[:7]})")
        return records, sha
    except Exception as exc:
        log_warn(f"  Could not parse {path} from GitHub ({exc}) — treating as empty")
        return [], sha


def _gh_push_file(
    token: str,
    repo: str,
    path: str,
    branch: str,
    content_bytes: bytes,
    sha: str | None,
    commit_msg: str,
) -> None:
    payload: dict[str, Any] = {
        "message": commit_msg,
        "content": base64.b64encode(content_bytes).decode("ascii"),
        "branch":  branch,
    }
    if sha:
        payload["sha"] = sha

    api_path = f"/repos/{repo}/contents/{path}"
    _gh_api_request("PUT", api_path, token, payload=payload, timeout=60)
    action = "updated" if sha else "created"
    log_ok(f"  GitHub → {path} {action} ({len(content_bytes):,} B)")


def _gh_get_text_file(
    token: str,
    repo: str,
    path: str,
    branch: str,
) -> tuple[str, str | None]:
    api_path = f"/repos/{repo}/contents/{path}?ref={branch}"
    try:
        meta = _gh_api_request("GET", api_path, token)
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return "", None
        raise

    raw_b64 = meta.get("content", "").replace("\n", "")
    sha     = meta.get("sha")
    if not raw_b64:
        return "", sha
    try:
        text = base64.b64decode(raw_b64).decode("utf-8", errors="replace")
        return text, sha
    except Exception as exc:
        log_warn(f"  Could not decode {path} from GitHub ({exc}) — treating as empty")
        return "", sha


def github_sync_error_log(
    token: str,
    repo: str,
    branch: str,
    filename: str = ERROR_LOG_GH_FILENAME,
) -> None:
    block = _format_error_log_block()
    if not block:
        return

    existing_text, sha = _gh_get_text_file(token, repo, filename, branch)
    merged = existing_text + block

    merged_bytes = merged.encode("utf-8")
    if len(merged_bytes) > ERROR_LOG_GH_SIZE_LIMIT:
        merged_bytes = merged_bytes[-ERROR_LOG_GH_SIZE_LIMIT:]
        merged = "[... older entries trimmed ...]\n" + merged_bytes.decode("utf-8", errors="ignore")
        merged_bytes = merged.encode("utf-8")

    commit_msg = f"Update {filename} via pipeline [{datetime.now(timezone.utc).isoformat()}]"
    try:
        _gh_push_file(token, repo, filename, branch, merged_bytes, sha, commit_msg)
    except Exception as exc:
        log_err(f"  Failed to push {filename} to GitHub: {exc}")


def _gh_fetch_all_summary_files(
    token: str,
    repo: str,
    branch: str,
) -> tuple[list[dict[str, Any]], list[tuple[str, str | None]]]:
    all_records: list[dict[str, Any]] = []
    file_meta:   list[tuple[str, str | None]] = []

    for n in range(1, 9999):
        fname        = _gh_filename(n)
        records, sha = _gh_get_file(token, repo, fname, branch)
        file_meta.append((fname, sha))
        all_records.extend(records)
        if sha is None:
            break

    return all_records, file_meta


def _gh_split_records(records: list[dict[str, Any]]) -> list[bytes]:
    chunks:       list[bytes] = []
    current:      list[dict[str, Any]] = []
    current_size: int = 2

    for rec in records:
        rec_json = _format_summary_json([rec]).encode("utf-8")
        rec_size = len(rec_json) - 4

        if current and current_size + rec_size + 2 > GITHUB_FILE_SIZE_LIMIT:
            chunks.append(_format_summary_json(current).encode("utf-8"))
            current      = []
            current_size = 2

        current.append(rec)
        current_size += rec_size + 2

    if current:
        chunks.append(_format_summary_json(current).encode("utf-8"))

    return chunks if chunks else [b"[]\n"]


def github_sync_summary(
    stage1_options: list[ServerOption],
    stage2_results: list[dict[str, Any]],
    local_json_path: Path,
    token: str,
    repo: str,
    branch: str,
) -> None:
    log_head("STAGE 3  –  GitHub sync  →  " + repo)

    if not token or not repo:
        log_warn("GitHub Sync variables incomplete — skipping remote push.")
        return

    log_info(f"Fetching existing summary files from {repo} (branch: {branch})…")
    try:
        remote_records, file_meta = _gh_fetch_all_summary_files(token, repo, branch)
    except Exception as exc:
        log_err(f"Failed to fetch from GitHub: {exc}")
        return

    log_info(f"Remote total: {len(remote_records)} entries across {len(file_meta)} file(s)")

    link_map = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}
    new_groups_raw: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups_raw[tmdb][opt.api_url] = {"host": urlparse(stream_url).netloc, "url": stream_url, "key": opt.api_url}
    new_groups: dict[str, list[dict[str, Any]]] = {
        tmdb: list(url_map.values()) for tmdb, url_map in new_groups_raw.items()
    }

    index: dict[int, dict[str, Any]] = {}
    for e in remote_records:
        tmdb_int, imdb_id, title, extracted_at, sources = _parse_entry_from_record(e)
        if not tmdb_int:
            continue
        index[tmdb_int] = {
            "tmdb_id":      tmdb_int,
            "imdb_id":      imdb_id,
            "title":        title,
            "extracted_at": extracted_at,
            "_sources":     sources,
        }

    extracted_at     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmdb_meta_cache: dict[int, tuple[str, Any]] = {}

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)
        if tmdb_int in index:
            entry         = index[tmdb_int]
            existing_keys = {s["key"] for s in entry["_sources"]}
            added         = [s for s in new_sources if s["key"] not in existing_keys]
            entry["_sources"].extend(added)
            entry["extracted_at"] = extracted_at
            log_info(f"  tmdb={tmdb_int} — merged {len(added)} new source(s)")
        else:
            if tmdb_int not in tmdb_meta_cache:
                log_info(f"  tmdb={tmdb_int} — fetching title + imdb_id…")
                title, imdb_id = _fetch_tmdb_info(tmdb_str)
                tmdb_meta_cache[tmdb_int] = (title, imdb_id)
                log_ok(f"  tmdb={tmdb_int} — '{title}'  imdb={imdb_id}")
            else:
                title, imdb_id = tmdb_meta_cache[tmdb_int]
            index[tmdb_int] = {
                "tmdb_id":      tmdb_int,
                "imdb_id":      imdb_id,
                "title":        title,
                "extracted_at": extracted_at,
                "_sources":     list(new_sources),
            }
            log_ok(f"  tmdb={tmdb_int} — '{title}'  sources: {len(new_sources)}")

    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    output: list[dict[str, Any]] = []
    for e in sorted_entries:
        tmdb_val = str(e["tmdb_id"])
        imdb_val = str(e.get("imdb_id") or "")
        tmdb_imdb_val = f"{tmdb_val}/{imdb_val}" if imdb_val else f"{tmdb_val}/"

        row: dict[str, Any] = {
            "serial":       e["serial"],
            "title":        e.get("title", ""),
            "tmdb/imdb":    tmdb_imdb_val,
            "extracted_at": e["extracted_at"],
        }
        for n, src in enumerate(e["_sources"], 1):
            row[f"host-{n}"] = src["url"]
        output.append(row)

    total_sources = sum(sum(1 for k in r if k.startswith("host-")) for r in output)
    log_info(f"Merged total: {len(output)} movies, {total_sources} sources")

    chunks = _gh_split_records(output)
    log_info(f"Split into {len(chunks)} file(s) ({GITHUB_FILE_SIZE_LIMIT // 1024 // 1024} MB limit each)")

    for i, chunk_bytes in enumerate(chunks, 1):
        target_local = _split_part_path(local_json_path, i)
        target_local.write_bytes(chunk_bytes)
        log_ok(f"Local JSON → {target_local}  ({len(chunk_bytes):,} B)")

    while len(file_meta) < len(chunks):
        n = len(file_meta) + 1
        file_meta.append((_gh_filename(n), None))

    pushed = 0
    for i, chunk_bytes in enumerate(chunks):
        fname, sha = file_meta[i]
        commit_msg = f"Update {fname} via pipeline [{extracted_at}]" if sha else f"Create {fname} via pipeline [{extracted_at}]"
        try:
            _gh_push_file(token, repo, fname, branch, chunk_bytes, sha, commit_msg)
            pushed += 1
        except Exception as exc:
            log_err(f"  Failed to push {fname}: {exc}")

    log_ok(f"GitHub sync complete — {pushed}/{len(chunks)} file(s) pushed")


# ═══════════════════════════════════════════════════════════════
# CLI ENTRYPOINT
# ═══════════════════════════════════════════════════════════════

def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PrimeSrc unified pipeline: embed URLs → API keys → stream URLs")
    p.add_argument("--input",                 type=Path, default=DEFAULT_INPUT_FILE)
    p.add_argument("--latest-input",          type=Path, default=DEFAULT_HOLLY_BOLLY_INPUT,  dest="latest_input")
    p.add_argument("--include-manual-input",  action="store_true", default=True,             dest="include_manual_input", help="Process tmdb_movie_input_list.txt (default: True)")
    p.add_argument("--no-manual-input",       action="store_false", dest="include_manual_input", help="Skip tmdb_movie_input_list.txt (used for automatic scheduled runs)")
    p.add_argument("--include-latest-input",  action="store_true", default=True,             dest="include_latest_input", help="Process lastet_released_holly_bolly_movies_list.txt (default: True)")
    p.add_argument("--no-latest-input",       action="store_false", dest="include_latest_input", help="Skip lastet_released_holly_bolly_movies_list.txt")
    p.add_argument("--fetch-latest",          action="store_true", default=True,             dest="fetch_latest", help="Fetch latest Hollywood/Bollywood movies via TMDB (default: True)")
    p.add_argument("--no-fetch-latest",       action="store_false", dest="fetch_latest",     help="Disable fetching latest movies via TMDB")
    p.add_argument("--latest-limit",          type=int,  default=50,                         dest="latest_limit", help="Number of latest movies to fetch per run (default: 50)")
    p.add_argument("--api-list-found",        type=Path, default=DEFAULT_API_LIST_FOUND,     dest="api_list_found")
    p.add_argument("--api-list-not-found",    type=Path, default=DEFAULT_API_LIST_NOT_FOUND, dest="api_list_not_found")
    p.add_argument("--json-out",              type=Path, default=DEFAULT_JSON_SUMMARY)
    p.add_argument("--skip-stage1",           action="store_true", help="Skip Stage 1")
    p.add_argument("--skip-stage2",           action="store_true", help="Skip Stage 2; only collect keys, no FlareSolverr")
    p.add_argument("--type",                  choices=("movie", "tv"), default="movie")
    p.add_argument("--flaresolverr-url",      default=None, dest="flaresolverr_url")
    p.add_argument("--fs-timeout",            type=int,   default=FLARESOLVERR_MAX_TIMEOUT, dest="fs_timeout_ms")
    p.add_argument("--batch-size",            type=int,   default=STAGE2_BATCH_SIZE,        dest="batch_size")
    p.add_argument("--batch-delay",           type=float, default=STAGE2_BATCH_DELAY,       dest="batch_delay")
    p.add_argument("--reloads",               type=int,   default=STAGE2_RELOADS)
    p.add_argument("--final-retries",         type=int,   default=STAGE2_FINAL_RETRIES,     dest="final_retries")
    p.add_argument("--error-log",             type=Path,  default=DEFAULT_ERROR_LOG,        dest="error_log")
    p.add_argument("--processed-urls",        type=Path,  default=DEFAULT_PROCESSED_URLS,   dest="processed_urls")
    p.add_argument("--no-github-sync",        action="store_true", default=False,           dest="no_github_sync")
    p.add_argument("--gh-token",              default=None, dest="gh_token")
    p.add_argument("--gh-repo",               default=None, dest="gh_repo")
    p.add_argument("--gh-branch",             default=None, dest="gh_branch")
    return p.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    log_head("PrimeSRC UNIFIED PIPELINE")

    # Auto-create all input and output files if missing
    _ensure_file_exists(args.input, "")
    _ensure_file_exists(args.latest_input, "")
    _ensure_file_exists(args.api_list_found, "")
    _ensure_file_exists(args.api_list_not_found, "")
    _ensure_file_exists(args.processed_urls, "")
    _ensure_file_exists(args.error_log, "")
    _ensure_file_exists(args.json_out, "[]\n")

    # Fetch TMDB Now Playing and Top Rated movies if enabled
    if args.fetch_latest and args.include_latest_input:
        log_head(f"FETCHING TMDB NOW PLAYING & TOP RATED MOVIES (Limit: {args.latest_limit})")
        fetch_tmdb_now_playing_and_top_rated(
            target_file=args.latest_input,
            existing_input_files=[args.input],
            processed_urls_file=args.processed_urls,
            limit=args.latest_limit,
        )

    all_input_files: list[Path] = []
    if args.include_manual_input:
        all_input_files.append(args.input)
    if args.include_latest_input:
        all_input_files.append(args.latest_input)

    log_info(f"Active input file(s) : {', '.join(p.name for p in all_input_files) if all_input_files else 'None'}")
    log_info(f"API list found       : {args.api_list_found}")
    log_info(f"API list not found   : {args.api_list_not_found}")

    stage1_options: list[ServerOption] = []
    stage2_results: list[dict[str, Any]] = []

    gh_token  = args.gh_token  or os.environ.get("GH_TOKEN", "")
    gh_repo   = args.gh_repo   or os.environ.get("GH_REPO",  "")
    gh_branch = args.gh_branch or os.environ.get("GH_BRANCH", "main")
    gh_available = not args.no_github_sync and bool(gh_token) and bool(gh_repo)

    try:
        if args.skip_stage1 or not all_input_files:
            if not all_input_files:
                log_warn("No input files selected — skipping Stage 1.")
            else:
                log_info("Stage 1 skipped.")
        else:
            stage1_options = stage1_fetch_api_keys(
                all_input_files,
                args.processed_urls,
                args.type,
                args.api_list_found,
                args.api_list_not_found,
            )

        if args.skip_stage2:
            log_info("Stage 2 skipped.")
        else:
            if not stage1_options:
                log_warn("No keys from Stage 1 — skipping Stage 2.")
            else:
                try:
                    stage2_results = await stage2_extract_stream_urls(
                        stage1_options, args
                    )
                except ConnectionError:
                    log_err("FlareSolverr unreachable – verification failed.")
                    return 2

        if stage1_options or stage2_results:
            if gh_available:
                github_sync_summary(stage1_options, stage2_results, args.json_out, gh_token, gh_repo, gh_branch)
            else:
                if not args.no_github_sync and not gh_token:
                    log_warn("GH_TOKEN not set — writing locally only")
                _write_summary(stage1_options, stage2_results, args.json_out)

        log_head("DONE")
        if not args.skip_stage2 and stage2_results:
            ok = sum(1 for r in stage2_results if r.get("extracted_url"))
            log_ok(f"Stream URLs extracted : {ok} / {len(stage2_results)}")
        return 0
    finally:
        write_error_log(args.error_log)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())

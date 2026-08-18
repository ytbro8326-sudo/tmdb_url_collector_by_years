#!/usr/bin/env python3
"""
tmdb_movie_extractor.py
=======================
Direct TMDB Movie Extractor for Hollywood & Bollywood/Indian Cinema (1950–2026).
Extracts TMDB IDs and saves them in PrimeSrc embed URL format:
    https://primesrc.me/embed/movie?tmdb=<id>

Output Files:
    hollywood_<year>_list.txt
    bollywood_indian_<year>_list.txt
    higest_grossing_hollywood_movies_<year>_list.txt
    higest_grossing_bollywood_indian_movies_<year>_list.txt

Features:
    - Extracts popularity-sorted and highest-grossing (revenue-sorted) movie lists.
    - GitHub Actions ready (CI/CD workflows, workflow_dispatch, schedule).
    - Built-in GitHub REST API sync to commit/upload output files directly to repository.
"""

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, List, Optional, Set
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import urllib.error

# ═══════════════════════════════════════════════════════════════
# DIRECT CONFIGURATION — DEFAULTS & ENVIRONMENT VARIABLES
# ═══════════════════════════════════════════════════════════════

START_YEAR = int(os.environ.get("START_YEAR", 2025))
END_YEAR   = int(os.environ.get("END_YEAR", 2025))
CATEGORY   = os.environ.get("CATEGORY", "both").lower()  # Options: "both", "hollywood", "bollywood", "all"
EXTRACT_HIGHEST_GROSSING = os.environ.get("EXTRACT_HIGHEST_GROSSING", "true").lower() in ("true", "1", "yes")

# API & Output Settings
TMDB_API_KEY        = os.environ.get("TMDB_API_KEY", "6fad3f86b8452ee232deb7977d7dcf58")
OUTPUT_DIR          = Path(os.environ.get("OUTPUT_DIR", str(Path(__file__).parent / "tmdb_movie_lists")))
MAX_PAGES_PER_YEAR  = int(os.environ.get("MAX_PAGES_PER_YEAR", 500))  # Max TMDB pages to pull per category/year
DELAY_BETWEEN_PAGES = float(os.environ.get("DELAY_BETWEEN_PAGES", 0.15))  # Delay in seconds between page requests

# GitHub API Settings
GH_TOKEN  = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
GH_REPO   = os.environ.get("GH_REPO") or os.environ.get("GITHUB_REPOSITORY") or ""
GH_BRANCH = os.environ.get("GH_BRANCH") or os.environ.get("GITHUB_REF_NAME") or "main"
GITHUB_API_ROOT = "https://api.github.com"

# Indian languages for Bollywood & Regional Indian cinema
INDIAN_LANGUAGES = "hi|te|ta|ml|kn|bn|pa|mr|gu|or|as"
TMDB_BASE_URL    = "https://api.themoviedb.org/3"

# ═══════════════════════════════════════════════════════════════
# CONSOLE HELPERS
# ═══════════════════════════════════════════════════════════════

_RESET  = "\033[0m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"

def _c(text: str, col: str) -> str:
    return f"{col}{text}{_RESET}" if sys.stdout.isatty() else text

def log_info(msg: str) -> None: print(_c(f"[INFO]  {msg}", _CYAN))
def log_ok(msg: str)   -> None: print(_c(f"[OK]    {msg}", _GREEN))
def log_warn(msg: str) -> None: print(_c(f"[WARN]  {msg}", _YELLOW))
def log_err(msg: str)  -> None: print(_c(f"[ERR]   {msg}", _RED))
def log_head(msg: str) -> None: print(_c(f"\n{'='*60}\n{msg}\n{'='*60}", _BOLD))


# ═══════════════════════════════════════════════════════════════
# TMDB API REQUEST
# ═══════════════════════════════════════════════════════════════

def tmdb_request(path: str, params: dict[str, Any], api_key: str, retries: int = 4) -> dict[str, Any]:
    params = dict(params)
    params["api_key"] = api_key
    params.setdefault("language", "en-US")
    url = f"{TMDB_BASE_URL}{path}?{urlencode(params)}"

    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json",
        },
    )

    for attempt in range(retries):
        try:
            with urlopen(req, timeout=15) as resp:
                data = resp.read().decode("utf-8", errors="replace")
                return json.loads(data)
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                wait_sec = 2.0 * (attempt + 1)
                log_warn(f"Rate limited (429) on {path}, waiting {wait_sec:.1f}s...")
                time.sleep(wait_sec)
                continue
            if attempt == retries - 1:
                log_err(f"HTTP {exc.code} for {url}: {exc}")
                return {}
            time.sleep(1.0)
        except Exception as exc:
            if attempt == retries - 1:
                log_err(f"Request error for {url}: {exc}")
                return {}
            time.sleep(1.0)

    return {}


# ═══════════════════════════════════════════════════════════════
# MOVIE EXTRACTION PER YEAR
# ═══════════════════════════════════════════════════════════════

def extract_movies_for_year(
    category: str,
    year: int,
    api_key: str,
    sort_by: str = "popularity.desc",
    max_pages: int = MAX_PAGES_PER_YEAR,
    delay: float = DELAY_BETWEEN_PAGES,
    label: str = "",
) -> list[str]:
    params: dict[str, Any] = {
        "primary_release_year": year,
        "sort_by": sort_by,
        "include_adult": "false",
        "include_video": "false",
    }

    if category == "hollywood":
        params["with_origin_country"] = "US"
        params["with_original_language"] = "en"
    elif category == "bollywood":
        params["with_origin_country"] = "IN"
        params["with_original_language"] = INDIAN_LANGUAGES
    else:
        raise ValueError(f"Unknown category: {category}")

    seen_ids: Set[str] = set()
    embed_urls: list[str] = []

    page = 1
    total_pages = 1
    display_tag = label or f"{category.upper()} {year} ({sort_by})"

    while page <= total_pages and page <= max_pages:
        params["page"] = page
        data = tmdb_request("/discover/movie", params, api_key)
        if not data:
            break

        total_pages = min(data.get("total_pages", 1), max_pages)
        results = data.get("results", [])
        if not results:
            break

        for m in results:
            mid = str(m.get("id") or "").strip()
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                embed_urls.append(f"https://primesrc.me/embed/movie?tmdb={mid}")

        if page % 10 == 0 or page == total_pages:
            print(f"    [{display_tag}] Page {page:>3}/{total_pages} — Collected {len(embed_urls)} movies", end="\r")

        page += 1
        if delay > 0:
            time.sleep(delay)

    print()
    return embed_urls


# ═══════════════════════════════════════════════════════════════
# GITHUB BUILT-IN REST API INTEGRATION
# ═══════════════════════════════════════════════════════════════

def github_api_get_file_sha(repo: str, file_path_in_repo: str, token: str) -> Optional[str]:
    """Retrieve existing file SHA if file already exists in repository."""
    url = f"{GITHUB_API_ROOT}/repos/{repo}/contents/{file_path_in_repo}"
    req = Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "TMDB-Movie-Extractor",
        },
    )
    try:
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("sha")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None  # File does not exist yet
        log_warn(f"GitHub API check SHA error for {file_path_in_repo}: {exc}")
        return None
    except Exception as exc:
        log_warn(f"GitHub API check SHA error for {file_path_in_repo}: {exc}")
        return None


def github_api_upload_file(
    local_file: Path,
    repo_rel_path: str,
    repo: str,
    token: str,
    branch: str = "main",
    commit_msg: str = "",
) -> bool:
    """Commit or update a file in the GitHub repo directly using GitHub REST API."""
    if not local_file.exists():
        log_warn(f"Local file does not exist to sync: {local_file}")
        return False

    content_bytes = local_file.read_bytes()
    b64_content = base64.b64encode(content_bytes).decode("ascii")

    # Clean path for GitHub API (forward slashes)
    clean_repo_path = repo_rel_path.replace("\\", "/").lstrip("/")
    url = f"{GITHUB_API_ROOT}/repos/{repo}/contents/{clean_repo_path}"

    sha = github_api_get_file_sha(repo, clean_repo_path, token)

    payload: dict[str, Any] = {
        "message": commit_msg or f"Update {clean_repo_path} [TMDB Extractor]",
        "content": b64_content,
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha

    req = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="PUT",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "TMDB-Movie-Extractor",
        },
    )

    try:
        with urlopen(req, timeout=30) as resp:
            status_code = resp.getcode()
            if status_code in (200, 201):
                action = "Updated" if sha else "Created"
                log_ok(f"GitHub API: {action} {clean_repo_path} ({len(content_bytes):,} bytes) on branch '{branch}'")
                return True
            else:
                log_warn(f"GitHub API returned HTTP {status_code} for {clean_repo_path}")
                return False
    except urllib.error.HTTPError as exc:
        err_msg = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else str(exc)
        log_err(f"GitHub API upload failed ({exc.code}) for {clean_repo_path}: {err_msg}")
        return False
    except Exception as exc:
        log_err(f"GitHub API upload failed for {clean_repo_path}: {exc}")
        return False


def sync_output_files_to_github(
    saved_files: list[Path],
    repo: str,
    token: str,
    branch: str = "main",
) -> None:
    """Sync all extracted movie list files to the main GitHub repository using GitHub API."""
    if not token or not repo:
        log_warn("GitHub API Sync skipped: GH_TOKEN / GH_REPO not provided.")
        return

    log_head("SYNCING OUTPUT FILES TO GITHUB REPO (API)")
    log_info(f"Target Repo   : {repo}")
    log_info(f"Target Branch : {branch}")
    log_info(f"Files to sync : {len(saved_files)}")

    # Determine repo-relative path base
    script_dir = Path(__file__).parent.resolve()

    success_count = 0
    for file_path in saved_files:
        try:
            rel_to_script = file_path.resolve().relative_to(script_dir)
            repo_path = f"vixsrcrender/{rel_to_script}"
        except ValueError:
            repo_path = f"vixsrcrender/tmdb_movie_lists/{file_path.name}"

        ok = github_api_upload_file(
            local_file=file_path,
            repo_rel_path=repo_path,
            repo=repo,
            token=token,
            branch=branch,
            commit_msg=f"Update movie list: {file_path.name}",
        )
        if ok:
            success_count += 1

    log_ok(f"GitHub API Sync complete: {success_count}/{len(saved_files)} files synced successfully.")


# ═══════════════════════════════════════════════════════════════
# MAIN EXTRACTOR RUNNER
# ═══════════════════════════════════════════════════════════════

def run_extractor(
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    category: str = CATEGORY,
    extract_highest_grossing: bool = EXTRACT_HIGHEST_GROSSING,
    max_pages: int = MAX_PAGES_PER_YEAR,
    api_key: str = TMDB_API_KEY,
    output_dir: Path = OUTPUT_DIR,
    sync_to_github: bool = True,
    gh_token: str = GH_TOKEN,
    gh_repo: str = GH_REPO,
    gh_branch: str = GH_BRANCH,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    cat_lower = category.lower()
    if cat_lower in ("both", "all"):
        do_hollywood = True
        do_bollywood = True
    elif cat_lower == "hollywood":
        do_hollywood = True
        do_bollywood = False
    elif cat_lower in ("bollywood", "indian"):
        do_hollywood = False
        do_bollywood = True
    else:
        do_hollywood = True
        do_bollywood = True

    log_head(f"TMDB MOVIE EXTRACTOR: {start_year} TO {end_year}")
    log_info(f"Hollywood extraction         : {'Enabled' if do_hollywood else 'Disabled'}")
    log_info(f"Bollywood/Indian extraction  : {'Enabled' if do_bollywood else 'Disabled'}")
    log_info(f"Highest Grossing extraction  : {'Enabled' if extract_highest_grossing else 'Disabled'}")
    log_info(f"Max Pages per year           : {max_pages}")
    log_info(f"Output directory             : {output_dir}")
    log_info(f"Year range                   : {start_year} -> {end_year}")

    total_hollywood = 0
    total_bollywood = 0
    total_hg_hollywood = 0
    total_hg_bollywood = 0
    saved_files: list[Path] = []

    for year in range(start_year, end_year + 1):
        log_head(f"YEAR: {year}")

        # ── 1. Hollywood (Popularity) ─────────────────────────────────
        if do_hollywood:
            h_file = output_dir / f"hollywood_{year}_list.txt"
            log_info(f"Extracting Hollywood (Popularity) for {year}...")
            h_urls = extract_movies_for_year(
                category="hollywood",
                year=year,
                api_key=api_key,
                sort_by="popularity.desc",
                max_pages=max_pages,
                label=f"HOLLYWOOD POPULAR {year}",
            )
            if h_urls:
                h_file.write_text("\n".join(h_urls) + "\n", encoding="utf-8")
                log_ok(f"Saved {len(h_urls)} Hollywood URLs -> {h_file.name}")
                total_hollywood += len(h_urls)
                saved_files.append(h_file)
            else:
                log_warn(f"No Hollywood movies found for {year}")

        # ── 2. Bollywood / Indian (Popularity) ────────────────────────
        if do_bollywood:
            b_file = output_dir / f"bollywood_indian_{year}_list.txt"
            log_info(f"Extracting Bollywood/Indian (Popularity) for {year}...")
            b_urls = extract_movies_for_year(
                category="bollywood",
                year=year,
                api_key=api_key,
                sort_by="popularity.desc",
                max_pages=max_pages,
                label=f"BOLLYWOOD POPULAR {year}",
            )
            if b_urls:
                b_file.write_text("\n".join(b_urls) + "\n", encoding="utf-8")
                log_ok(f"Saved {len(b_urls)} Bollywood/Indian URLs -> {b_file.name}")
                total_bollywood += len(b_urls)
                saved_files.append(b_file)
            else:
                log_warn(f"No Bollywood/Indian movies found for {year}")

        # ── 3. Highest Grossing Hollywood (Revenue) ───────────────────
        if do_hollywood and extract_highest_grossing:
            hg_h_file = output_dir / f"higest_grossing_hollywood_movies_{year}_list.txt"
            log_info(f"Extracting Highest Grossing Hollywood for {year}...")
            hg_h_urls = extract_movies_for_year(
                category="hollywood",
                year=year,
                api_key=api_key,
                sort_by="revenue.desc",
                max_pages=max_pages,
                label=f"HIGHEST GROSSING HOLLYWOOD {year}",
            )
            if hg_h_urls:
                hg_h_file.write_text("\n".join(hg_h_urls) + "\n", encoding="utf-8")
                log_ok(f"Saved {len(hg_h_urls)} Highest Grossing Hollywood URLs -> {hg_h_file.name}")
                total_hg_hollywood += len(hg_h_urls)
                saved_files.append(hg_h_file)
            else:
                log_warn(f"No Highest Grossing Hollywood movies found for {year}")

        # ── 4. Highest Grossing Bollywood / Indian (Revenue) ──────────
        if do_bollywood and extract_highest_grossing:
            hg_b_file = output_dir / f"higest_grossing_bollywood_indian_movies_{year}_list.txt"
            log_info(f"Extracting Highest Grossing Bollywood/Indian for {year}...")
            hg_b_urls = extract_movies_for_year(
                category="bollywood",
                year=year,
                api_key=api_key,
                sort_by="revenue.desc",
                max_pages=max_pages,
                label=f"HIGHEST GROSSING BOLLYWOOD {year}",
            )
            if hg_b_urls:
                hg_b_file.write_text("\n".join(hg_b_urls) + "\n", encoding="utf-8")
                log_ok(f"Saved {len(hg_b_urls)} Highest Grossing Bollywood/Indian URLs -> {hg_b_file.name}")
                total_hg_bollywood += len(hg_b_urls)
                saved_files.append(hg_b_file)
            else:
                log_warn(f"No Highest Grossing Bollywood/Indian movies found for {year}")

    log_head("COMPLETED")
    if do_hollywood:
        log_ok(f"Total Hollywood movies (Popular)           : {total_hollywood:,}")
    if do_bollywood:
        log_ok(f"Total Bollywood/Indian movies (Popular)    : {total_bollywood:,}")
    if do_hollywood and extract_highest_grossing:
        log_ok(f"Total Highest Grossing Hollywood movies    : {total_hg_hollywood:,}")
    if do_bollywood and extract_highest_grossing:
        log_ok(f"Total Highest Grossing Bollywood movies    : {total_hg_bollywood:,}")
    log_info(f"Files saved in: {output_dir}")

    # GitHub built-in REST API sync
    if sync_to_github and saved_files and gh_token and gh_repo:
        sync_output_files_to_github(saved_files, repo=gh_repo, token=gh_token, branch=gh_branch)

    return saved_files


# ═══════════════════════════════════════════════════════════════
# EXECUTION & CLI PARSER
# ═══════════════════════════════════════════════════════════════

def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TMDB Movie Extractor: extracts TMDB movie URLs for Hollywood & Bollywood/Indian cinema (Popular & Highest Grossing)."
    )
    parser.add_argument("start_year", nargs="?", type=int, default=START_YEAR, help=f"Start year (default: {START_YEAR})")
    parser.add_argument("end_year", nargs="?", type=int, default=None, help=f"End year (default: same as start_year)")
    parser.add_argument("category", nargs="?", type=str, default=CATEGORY, help=f"Category: both, hollywood, bollywood (default: {CATEGORY})")
    parser.add_argument("--highest-grossing", action="store_true", default=EXTRACT_HIGHEST_GROSSING, dest="highest_grossing", help="Include highest-grossing lists (revenue.desc)")
    parser.add_argument("--no-highest-grossing", action="store_false", dest="highest_grossing", help="Disable highest-grossing lists")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES_PER_YEAR, help=f"Max pages per category/year (default: {MAX_PAGES_PER_YEAR})")
    parser.add_argument("--api-key", type=str, default=TMDB_API_KEY, help="TMDB API Key")
    parser.add_argument("--output-dir", type=str, default=str(OUTPUT_DIR), help="Output directory path")
    parser.add_argument("--gh-token", type=str, default=GH_TOKEN, help="GitHub Token for REST API sync")
    parser.add_argument("--gh-repo", type=str, default=GH_REPO, help="GitHub repository (e.g. owner/repo)")
    parser.add_argument("--gh-branch", type=str, default=GH_BRANCH, help="GitHub branch (default: main)")
    parser.add_argument("--no-gh-sync", action="store_true", help="Disable GitHub API sync")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_cli_args()

    start = args.start_year
    end = args.end_year if args.end_year is not None else start
    cat = args.category.lower()
    hg = args.highest_grossing
    pages = args.max_pages
    api_key = args.api_key
    out_dir = Path(args.output_dir)
    sync_gh = not args.no_gh_sync

    run_extractor(
        start_year=start,
        end_year=end,
        category=cat,
        extract_highest_grossing=hg,
        max_pages=pages,
        api_key=api_key,
        output_dir=out_dir,
        sync_to_github=sync_gh,
        gh_token=args.gh_token,
        gh_repo=args.gh_repo,
        gh_branch=args.gh_branch,
    )



#!/usr/bin/env python3
"""
GitHub Push, Create, and Delete Management Utility
--------------------------------------------------
All-in-one GitHub management script to:
- Push local repository / code to GitHub (`push`)
- Create a new repository on GitHub (`create`)
- Delete a repository on GitHub (`delete`)
- Rename a repository on GitHub (`rename`)
- Create repo AND push local code in one step (`create-and-push`)
- List repositories (`list`)
- Fetch user info (`info`)
"""

import os
import sys
import json
import argparse
import subprocess
import urllib.request
import urllib.error

# Integrated GitHub Token and Default Owner from Environment
DEFAULT_TOKEN = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
DEFAULT_OWNER = os.environ.get("GITHUB_OWNER", "ytbro8326-sudo")

def get_headers(token=None):
    tok = token or DEFAULT_TOKEN
    return {
        "Authorization": f"token {tok}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "GitHub-Push-Create-Delete-Utility"
    }

def api_request(url, method="GET", data=None, token=None):
    headers = get_headers(token)
    req_body = json.dumps(data).encode("utf-8") if data else None
    
    req = urllib.request.Request(url, data=req_body, headers=headers, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
        
    try:
        with urllib.request.urlopen(req) as resp:
            content = resp.read().decode("utf-8")
            return json.loads(content) if content else {}
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8")
        try:
            parsed = json.loads(err_msg)
            message = parsed.get("message", err_msg)
        except Exception:
            message = err_msg
        raise Exception(f"GitHub API Error [{e.code}]: {message}")
    except Exception as e:
        raise Exception(f"Network error: {str(e)}")

def get_user_info(token=None):
    """Fetch info for authenticated user."""
    url = "https://api.github.com/user"
    return api_request(url, "GET", token=token)

def list_repos(token=None):
    """List all repositories owned by the user."""
    url = "https://api.github.com/user/repos?per_page=100&sort=updated"
    return api_request(url, "GET", token=token)

def create_repo(name, private=False, description="", token=None):
    """Create a new GitHub repository."""
    url = "https://api.github.com/user/repos"
    payload = {
        "name": name,
        "private": private,
        "description": description,
        "auto_init": False
    }
    return api_request(url, "POST", data=payload, token=token)

def rename_repo(old_name, new_name, owner=DEFAULT_OWNER, token=None):
    """Rename a GitHub repository."""
    url = f"https://api.github.com/repos/{owner}/{old_name}"
    payload = {"name": new_name}
    return api_request(url, "PATCH", data=payload, token=token)

def delete_repo(name, owner=DEFAULT_OWNER, token=None):
    """Delete a GitHub repository."""
    url = f"https://api.github.com/repos/{owner}/{name}"
    api_request(url, "DELETE", token=token)
    return True

def run_git(cmd, cwd=None):
    """Execute git CLI command."""
    result = subprocess.run(
        cmd,
        shell=True,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if result.returncode != 0:
        raise Exception(f"Git command failed: {cmd}\nOutput: {result.stderr or result.stdout}")
    return result.stdout.strip()

def push_code(repo_name, owner=DEFAULT_OWNER, path=".", branch="main", message="Update repository", force=False, token=None):
    """Initialize git, set remote origin with token, commit and push code."""
    tok = token or DEFAULT_TOKEN
    cwd = os.path.abspath(path)
    
    # 1. Initialize git if needed
    if not os.path.exists(os.path.join(cwd, ".git")):
        print(f"--> Initializing git repo in {cwd}...")
        run_git("git init", cwd=cwd)
    
    # 2. Set branch name
    run_git(f"git branch -M {branch}", cwd=cwd)

    # 3. Add all files
    print("--> Staging files...")
    run_git("git add .", cwd=cwd)
    
    # 4. Check if there are changes to commit
    status = subprocess.run("git status --porcelain", shell=True, cwd=cwd, text=True, stdout=subprocess.PIPE).stdout.strip()
    if status:
        print(f"--> Committing changes: '{message}'...")
        try:
            run_git("git config user.name", cwd=cwd)
        except Exception:
            run_git(f'git config user.name "{owner}"', cwd=cwd)
            run_git(f'git config user.email "{owner}@users.noreply.github.com"', cwd=cwd)

        run_git(f'git commit -m "{message}"', cwd=cwd)
    else:
        print("--> No new changes to commit.")

    # 5. Remote configuration
    remote_url = f"https://{tok}@github.com/{owner}/{repo_name}.git"
    
    try:
        run_git("git remote remove origin", cwd=cwd)
    except Exception:
        pass
        
    print(f"--> Setting remote origin for {owner}/{repo_name}...")
    run_git(f"git remote add origin {remote_url}", cwd=cwd)
    
    # 6. Push to remote
    push_cmd = f"git push -u origin {branch}"
    if force:
        push_cmd += " --force"
        
    print(f"--> Pushing to GitHub ({branch})...")
    run_git(push_cmd, cwd=cwd)
    print("--> Push successful!")

def create_and_push(repo_name, private=False, description="", path=".", branch="main", message="Initial commit", force=False, token=None):
    """Create a repo on GitHub and push local directory in one step."""
    owner = DEFAULT_OWNER
    try:
        user_info = get_user_info(token)
        owner = user_info.get("login", DEFAULT_OWNER)
    except Exception:
        pass

    print(f"==> Creating repository '{repo_name}' on GitHub (Private={private})...")
    try:
        created = create_repo(repo_name, private=private, description=description, token=token)
        print(f"Successfully created: {created.get('html_url')}")
    except Exception as e:
        if "already exists" in str(e).lower() or "422" in str(e):
            print(f"--> Repo '{repo_name}' already exists. Proceeding to push...")
        else:
            raise e

    print(f"==> Pushing local code from '{path}' to '{owner}/{repo_name}'...")
    push_code(repo_name, owner=owner, path=path, branch=branch, message=message, force=force, token=token)

def main():
    parser = argparse.ArgumentParser(description="GitHub Push, Create, & Delete CLI Utility")
    subparsers = parser.add_subparsers(dest="command")

    # push
    p_push = subparsers.add_parser("push", help="Push local directory to GitHub repository")
    p_push.add_argument("--repo", required=True, help="Repository name")
    p_push.add_argument("--path", default=".", help="Local path (default: .)")
    p_push.add_argument("--branch", default="main", help="Branch name (default: main)")
    p_push.add_argument("--message", "-m", default="Update code", help="Commit message")
    p_push.add_argument("--force", action="store_true", help="Force push")

    # create
    p_create = subparsers.add_parser("create", help="Create a new GitHub repository")
    p_create.add_argument("--repo", required=True, help="Repository name")
    p_create.add_argument("--private", action="store_true", help="Make repository private")
    p_create.add_argument("--desc", default="", help="Description")

    # delete
    p_del = subparsers.add_parser("delete", help="Delete a GitHub repository")
    p_del.add_argument("--repo", required=True, help="Repository name")

    # create-and-push
    p_cp = subparsers.add_parser("create-and-push", help="Create repository and push local directory")
    p_cp.add_argument("--repo", required=True, help="Repository name")
    p_cp.add_argument("--private", action="store_true", help="Make repository private")
    p_cp.add_argument("--desc", default="", help="Description")
    p_cp.add_argument("--path", default=".", help="Local path")
    p_cp.add_argument("--branch", default="main", help="Branch name")
    p_cp.add_argument("--message", "-m", default="Initial commit", help="Commit message")
    p_cp.add_argument("--force", action="store_true", help="Force push")

    # rename
    p_rn = subparsers.add_parser("rename", help="Rename a GitHub repository")
    p_rn.add_argument("--old", required=True, help="Old repository name")
    p_rn.add_argument("--new", required=True, help="New repository name")

    # list
    subparsers.add_parser("list", help="List user repositories")

    # info
    subparsers.add_parser("info", help="Get authenticated user info")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == "push":
            push_code(args.repo, path=args.path, branch=args.branch, message=args.message, force=args.force)
        elif args.command == "create":
            res = create_repo(args.repo, private=args.private, description=args.desc)
            print(f"Created repository: {res.get('html_url')}")
        elif args.command == "delete":
            delete_repo(args.repo)
            print(f"Successfully deleted repository: {args.repo}")
        elif args.command == "create-and-push":
            create_and_push(args.repo, private=args.private, description=args.desc, path=args.path, branch=args.branch, message=args.message, force=args.force)
        elif args.command == "rename":
            res = rename_repo(args.old, args.new)
            print(f"Renamed {args.old} -> {args.new}: {res.get('html_url')}")
        elif args.command == "list":
            repos = list_repos()
            print(f"Found {len(repos)} repositories:")
            for r in repos:
                print(f" - {r.get('full_name')} ({'Private' if r.get('private') else 'Public'})")
        elif args.command == "info":
            info = get_user_info()
            print(f"User: {info.get('login')} | Repos: {info.get('public_repos')}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

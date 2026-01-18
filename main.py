import os
import json
import yaml
import requests
import threading
from pathlib import Path
import concurrent.futures
from typing import Dict, List
from urllib.parse import urlencode
from datetime import datetime, timezone


GITHUB_TOKEN = os.getenv("GH_TOKEN")
GH_HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}


def load_config():
    config_path = Path("config/settings.yml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    config.setdefault("clickhouse", {})
    config["clickhouse"]["url"] = os.getenv("CLICKHOUSE_URL") or config[
        "clickhouse"
    ].get("url", "https://play.clickhouse.com")

    config["clickhouse"]["table"] = os.getenv("CLICKHOUSE_TABLE") or config[
        "clickhouse"
    ].get("table", "github_events")

    config["clickhouse"]["timeout"] = float(
        os.getenv("CLICKHOUSE_TIMEOUT", str(config["clickhouse"].get("timeout", 60)))
    )

    config.setdefault("processing", {})

    recent_repos_limit_value = config["processing"].get("recent_repos_limit")

    if recent_repos_limit_value is None:
        config["processing"]["recent_repos_limit"] = float("inf")
    else:
        config["processing"]["recent_repos_limit"] = int(
            os.getenv("RECENT_REPOS_LIMIT", str(recent_repos_limit_value))
        )

    max_workers_value = config["processing"].get("max_workers")

    if max_workers_value is None:
        config["processing"]["max_workers"] = 4
    else:
        config["processing"]["max_workers"] = int(
            os.getenv("MAX_WORKERS", str(max_workers_value))
        )

    top_n_value = config["processing"].get("top_n")

    if top_n_value is None:
        config["processing"]["top_n"] = float("inf")
    else:
        config["processing"]["top_n"] = int(os.getenv("TOP_N", str(top_n_value)))

    config.setdefault("user", {})
    config["user"]["login"] = os.getenv("GH_USER") or config["user"].get("login")

    if not config["user"]["login"]:
        raise RuntimeError(
            "GitHub username not configured (GH_USER or config.user.login)"
        )

    return config


config = load_config()

RECOMMENDATIONS_DIR = Path(config["paths"]["recommendations_dir"])
LATEST_JSON = Path(config["paths"]["latest_json"])

CLICKHOUSE_URL = config["clickhouse"]["url"]
CLICKHOUSE_TABLE = config["clickhouse"]["table"]
CLICKHOUSE_TIMEOUT = config["clickhouse"]["timeout"]

RECENT_REPOS_LIMIT = config["processing"]["recent_repos_limit"]
MAX_WORKERS = config["processing"]["max_workers"]
TOP_N = config["processing"]["top_n"]

USER_LOGIN = config["user"]["login"]

progress_lock = threading.Lock()
progress_counter = 0


class ClickHouseError(RuntimeError):
    pass


def fetch_user_starred(username: str) -> List[str]:
    repos = []
    page = 1

    while True:
        url = (
            f"https://api.github.com/users/{username}/starred?per_page=100&page={page}"
        )
        r = requests.get(url, timeout=20)

        if r.status_code != 200:
            raise RuntimeError(f"GitHub API error {r.status_code}: {r.text}")

        batch = r.json()
        if not batch:
            break

        repos.extend(repo["full_name"] for repo in batch)
        page += 1

    return repos


def run_query(sql: str):
    params = {"default_format": "JSONEachRow", "user": "explorer"}
    url = f"{CLICKHOUSE_URL}/?{urlencode(params)}"

    for attempt in range(5):
        try:
            r = requests.post(url, data=sql.encode(), timeout=CLICKHOUSE_TIMEOUT)
            if r.status_code != 200:
                raise ClickHouseError(r.text)
            return [json.loads(x) for x in r.text.splitlines() if x.strip()]
        except Exception:
            if attempt == 4:
                raise


def literal(x: str) -> str:
    return "'" + x.replace("\\", "\\\\").replace("'", "\\'") + "'"


def fetch_total_forks(repos: List[str]) -> Dict[str, int]:
    if not repos:
        return {}

    sql = f"""
        SELECT repo_name, count() AS total_forks
        FROM {CLICKHOUSE_TABLE}
        WHERE event_type='ForkEvent'
          AND repo_name IN ({", ".join(literal(r) for r in repos)})
        GROUP BY repo_name
    """

    return {r["repo_name"]: int(r["total_forks"]) for r in run_query(sql)}


def fetch_total_stars(repos: List[str]) -> Dict[str, int]:
    if not repos:
        return {}

    sql = f"""
        SELECT repo_name, count() AS total_stars
        FROM {CLICKHOUSE_TABLE}
        WHERE event_type='WatchEvent'
          AND repo_name IN ({", ".join(literal(r) for r in repos)})
        GROUP BY repo_name
    """

    return {r["repo_name"]: int(r["total_stars"]) for r in run_query(sql)}


def process_repo(repo: str, total: int):
    global progress_counter

    with progress_lock:
        progress_counter += 1
        print(f"[{progress_counter}/{total}] Processing {repo}")

    limit_clause = "" if TOP_N == float("inf") else f"LIMIT {TOP_N}"

    sql = f"""
        SELECT 
            e.repo_name AS neighbor_repo,
            countDistinct(e.actor_login) AS forkers
        FROM {CLICKHOUSE_TABLE} e
        INNER JOIN (
            SELECT DISTINCT actor_login
            FROM {CLICKHOUSE_TABLE}
            WHERE event_type='ForkEvent'
              AND repo_name={literal(repo)}
        ) s USING actor_login
        WHERE e.event_type='ForkEvent'
          AND e.repo_name != {literal(repo)}
        GROUP BY neighbor_repo
        ORDER BY forkers DESC
        {limit_clause}
    """

    rows = run_query(sql)
    recs = [{"repo": r["neighbor_repo"], "count": int(r["forkers"])} for r in rows]

    star_totals = fetch_total_stars([r["repo"] for r in recs])
    fork_totals = fetch_total_forks([r["repo"] for r in recs])

    for r in recs:
        r["total_stars"] = star_totals.get(r["repo"], 0)
        r["total_forks"] = fork_totals.get(r["repo"], 0)
        ts = r["total_stars"]
        r["score"] = round(r["count"] / ts, 6) if ts > 0 else 0.0

    return {"repo": repo, "recommendations": recs}


def save_results(username: str, results):
    RECOMMENDATIONS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "username": username,
                "results": results,
            },
            indent=2,
        )
    )


def main():
    forked = fetch_user_starred(USER_LOGIN)  # ← switched source here
    total = len(forked)

    print(f"[INFO] Found {total} repos. Using {MAX_WORKERS} workers.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(lambda r: process_repo(r, total), forked))

    save_results(USER_LOGIN, results)
    print("[DONE] All repos processed.")


if __name__ == "__main__":
    main()

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


def load_config():
    config_path = Path("config/settings.yml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    config["clickhouse"]["url"] = os.getenv("CLICKHOUSE_URL") or config[
        "clickhouse"
    ].get("url", "https://play.clickhouse.com")

    config["clickhouse"]["table"] = os.getenv("CLICKHOUSE_TABLE") or config[
        "clickhouse"
    ].get("table", "github_events")

    config["clickhouse"]["timeout"] = float(
        os.getenv("CLICKHOUSE_TIMEOUT", str(config["clickhouse"].get("timeout", 60)))
    )

    config["processing"]["recent_repos_limit"] = int(
        os.getenv(
            "RECENT_REPOS_LIMIT",
            str(config["processing"].get("recent_repos_limit", 10)),
        )
    )

    config["processing"]["max_workers"] = int(
        os.getenv("MAX_WORKERS", str(config["processing"].get("max_workers", 4)))
    )

    config["processing"]["top_n"] = int(
        os.getenv("TOP_N", str(config["processing"].get("top_n", 10)))
    )

    config["user"]["login"] = os.getenv("GH_USER") or config["user"].get("login")

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


def run_query(sql: str):
    params = {"default_format": "JSONEachRow", "user": "explorer"}
    url = f"{CLICKHOUSE_URL}/?{urlencode(params)}"

    max_retries = 5

    for attempt in range(max_retries):
        try:
            r = requests.post(url, data=sql.encode(), timeout=CLICKHOUSE_TIMEOUT)

            if r.status_code != 200:
                if attempt < max_retries - 1:
                    print(
                        f"[WARN] API returned status {r.status_code}, sleeping for 3 seconds before retry {attempt + 1}/{max_retries}"
                    )
                    import time

                    time.sleep(3)
                    continue
                else:
                    raise ClickHouseError(
                        f"[WARN] API returned status {r.status_code} after {max_retries} attempts"
                    )

            r.raise_for_status()

            return [json.loads(x) for x in r.text.splitlines() if x.strip()]

        except Exception as e:
            if attempt < max_retries - 1:
                print(
                    f"[WARN] Attempt {attempt + 1} failed: {str(e)}, sleeping for 3 seconds before retry"
                )
                import time

                time.sleep(3)
            else:
                raise ClickHouseError(str(e))


def literal(x: str) -> str:
    return "'" + x.replace("\\", "\\\\").replace("'", "\\'") + "'"


def fetch_user_forks(username: str):
    sql = f"""
        SELECT repo_name, max(created_at) AS last_forked
        FROM {CLICKHOUSE_TABLE}
        WHERE event_type='ForkEvent'
          AND actor_login={literal(username)}
        GROUP BY repo_name
        ORDER BY last_forked DESC
        LIMIT {RECENT_REPOS_LIMIT}
    """
    return [r["repo_name"] for r in run_query(sql)]


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


def process_repo(repo: str, total: int):
    global progress_counter

    with progress_lock:
        progress_counter += 1
        idx = progress_counter

        print(f"[{idx}/{total}] Processing {repo}")

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
        LIMIT {TOP_N}
    """

    rows = run_query(sql)
    recs = []
    for r in rows:
        rec_data = {
            "repo": r["neighbor_repo"],
            "count": int(r["forkers"]),
        }

        repo_details_sql = f"""
            SELECT 
                repo_name,
                min(created_at) AS first_event_date,
                max(created_at) AS last_event_date,
                countIf(event_type='PushEvent') AS push_events,
                countIf(event_type='IssueCommentEvent') AS comment_events,
                countIf(event_type='IssuesEvent') AS issue_events,
                countIf(event_type='PullRequestEvent') AS pr_events,
                countIf(event_type='ForkEvent') AS fork_events,
                count() AS total_events
            FROM {CLICKHOUSE_TABLE}
            WHERE repo_name = {literal(r["neighbor_repo"])}
            GROUP BY repo_name
            LIMIT 1
        """

        details_result = run_query(repo_details_sql)
        if details_result:
            detail = details_result[0]
            rec_data.update(
                {
                    "first_event_date": detail.get("first_event_date"),
                    "last_event_date": detail.get("last_event_date"),
                    "push_events": int(detail.get("push_events", 0)),
                    "comment_events": int(detail.get("comment_events", 0)),
                    "issue_events": int(detail.get("issue_events", 0)),
                    "pr_events": int(detail.get("pr_events", 0)),
                    "fork_events": int(detail.get("fork_events", 0)),
                    "total_events": int(detail.get("total_events", 0)),
                }
            )

        recs.append(rec_data)

    totals = fetch_total_forks([r["repo"] for r in recs])

    for r in recs:
        r["total_forks"] = totals.get(r["repo"], 0)

    for r in recs:
        tf = r["total_forks"]
        r["normalized_score"] = round(r["count"] / tf, 6) if tf > 0 else 0.0

    return {"repo": repo, "recommendations": recs}


def save_results(username: str, results):
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "username": username,
        "results": results,
    }

    RECOMMENDATIONS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON.write_text(json.dumps(payload, indent=2))


def main():
    username = USER_LOGIN
    forked = fetch_user_forks(username)

    total = len(forked)
    print(
        f"[INFO] Found {total} repos. Starting parallel processing with {MAX_WORKERS} workers."
    )

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_repo, repo, total) for repo in forked]

        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    save_results(username, results)
    print("[DONE] All repos processed.")


if __name__ == "__main__":
    main()

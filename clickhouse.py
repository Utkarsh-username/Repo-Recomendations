import concurrent.futures
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode

import requests

RECOMMENDATIONS_DIR = Path("data/recommendations")
LATEST_JSON = RECOMMENDATIONS_DIR / "latest.json"
PER_REPO_DIR = Path("data/repo")

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "https://play.clickhouse.com")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "github_events")
CLICKHOUSE_TIMEOUT = float(os.getenv("CLICKHOUSE_TIMEOUT", "60"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "explorer")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")


def env_int(name: str) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return int(value)


def env_float(name: str) -> Optional[float]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return float(value)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


TOP_N = env_int("TOP_N") or 10
MIN_OVERLAP = env_int("MIN_COOCCURRENCE") or 1
MIN_STARGAZERS = env_int("MIN_STARGAZERS")
MIN_FORKERS = env_int("MIN_FORKERS")
MIN_RATIO = env_float("MIN_RATIO")
STARGAZERS_PER_REPO = env_int("STARGAZERS_PER_REPO")
STARS_PER_NEIGHBOR = env_int("STARS_PER_NEIGHBOR")
CLICKHOUSE_LIMIT = env_int("CLICKHOUSE_LIMIT")
ORDER_BY = os.getenv("CLICKHOUSE_ORDER_BY", "stargazers") or "stargazers"
if ORDER_BY not in {"stargazers", "forkers", "ratio"}:
    ORDER_BY = "stargazers"
MAX_WORKERS = 1
RECENT_REPOS_LIMIT = env_int("RECENT_REPOS_LIMIT") or 10
CLICKHOUSE_MAX_RETRIES = env_int("CLICKHOUSE_RETRIES") or 3
CLICKHOUSE_RETRY_BACKOFF = env_float("CLICKHOUSE_RETRY_BACKOFF") or 1.0


class ClickHouseError(RuntimeError):
    pass


def run_query(sql: str) -> List[Dict[str, str]]:
    params = {"default_format": "JSONEachRow"}
    if CLICKHOUSE_USER:
        params["user"] = CLICKHOUSE_USER
    if CLICKHOUSE_PASSWORD:
        params["password"] = CLICKHOUSE_PASSWORD

    url = f"{CLICKHOUSE_URL}/?{urlencode(params)}"

    last_error: Optional[Exception] = None
    for attempt in range(1, CLICKHOUSE_MAX_RETRIES + 1):
        try:
            resp = requests.post(
                url, data=sql.encode("utf-8"), timeout=CLICKHOUSE_TIMEOUT
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            last_error = exc
            if attempt == CLICKHOUSE_MAX_RETRIES:
                break
            sleep_for = CLICKHOUSE_RETRY_BACKOFF * attempt
            print(
                f"[WARN] ClickHouse request failed (attempt {attempt}/{CLICKHOUSE_MAX_RETRIES}): {exc}. Retrying in {sleep_for:.1f}s"
            )
            time.sleep(sleep_for)
            continue

        rows: List[Dict[str, str]] = []
        for line in resp.text.splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
        return rows

    raise ClickHouseError(str(last_error) if last_error else "Unknown ClickHouse error")


def literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def fetch_user_stars(username: str) -> List[str]:
    sql = f"""
        SELECT
            repo_name,
            max(created_at) AS last_starred
        FROM {CLICKHOUSE_TABLE}
        WHERE event_type = 'WatchEvent'
          AND actor_login = {literal(username)}
        GROUP BY repo_name
        ORDER BY last_starred DESC
    """
    if RECENT_REPOS_LIMIT:
        sql += f"\n        LIMIT {RECENT_REPOS_LIMIT}\n"
    rows = run_query(sql)
    return [row["repo_name"] for row in rows]


def build_repo_query(repo: str) -> str:
    repo_literal = literal(repo)
    watchers_limit_clause = (
        f"        AND rn_repo <= {STARGAZERS_PER_REPO}\n" if STARGAZERS_PER_REPO else ""
    )
    neighbor_limit_clause = (
        f"            WHERE rn_neighbor <= {STARS_PER_NEIGHBOR}\n"
        if STARS_PER_NEIGHBOR
        else ""
    )

    having_parts = [f"stargazers >= {MIN_OVERLAP}"]
    if MIN_STARGAZERS:
        having_parts.append(f"stargazers >= {MIN_STARGAZERS}")
    if MIN_FORKERS:
        having_parts.append(f"forkers >= {MIN_FORKERS}")
    if MIN_RATIO:
        having_parts.append(f"ratio >= {MIN_RATIO}")
    having_clause = " AND ".join(having_parts)

    limit_clause = ""
    if CLICKHOUSE_LIMIT:
        limit_clause = f"LIMIT {CLICKHOUSE_LIMIT}"

    return f"""
    WITH source AS (
        SELECT actor_login
        FROM (
            SELECT
                actor_login,
                row_number() OVER (PARTITION BY actor_login ORDER BY created_at DESC) AS rn_actor,
                row_number() OVER (ORDER BY created_at DESC) AS rn_repo
            FROM {CLICKHOUSE_TABLE}
            WHERE event_type = 'WatchEvent'
              AND repo_name = {repo_literal}
        )
        WHERE rn_actor = 1
{watchers_limit_clause}
        GROUP BY actor_login
    ),
    neighbor_events AS (
        SELECT
            e.repo_name AS neighbor_repo,
            e.event_type,
            e.actor_login,
            row_number() OVER (
                PARTITION BY e.actor_login
                ORDER BY e.created_at DESC
            ) AS rn_neighbor
        FROM {CLICKHOUSE_TABLE} e
        INNER JOIN source s ON e.actor_login = s.actor_login
        WHERE e.event_type IN ('WatchEvent', 'ForkEvent')
          AND e.repo_name != {repo_literal}
    ),
    filtered_neighbors AS (
        SELECT neighbor_repo, event_type, actor_login, rn_neighbor
        FROM neighbor_events
{neighbor_limit_clause}
    )
    SELECT
        neighbor_repo,
        count(DISTINCT IF(event_type = 'WatchEvent', actor_login, NULL)) AS stargazers,
        count(DISTINCT IF(event_type = 'ForkEvent', actor_login, NULL)) AS forkers,
        round(IF(forkers = 0, NULL, stargazers / forkers), 2) AS ratio
    FROM filtered_neighbors
    GROUP BY neighbor_repo
    HAVING {having_clause}
    ORDER BY {ORDER_BY} DESC
    {limit_clause}
    """


def fetch_repo_recommendations(repo: str) -> Dict[str, List[Dict[str, object]]]:
    sql = build_repo_query(repo)
    rows = run_query(sql)

    recommendations: List[Dict[str, object]] = []
    rich: List[Dict[str, object]] = []

    for row in rows:
        neighbor_repo = row.get("neighbor_repo")
        if not neighbor_repo:
            continue
        stargazers = int(row.get("stargazers", 0) or 0)
        forkers = int(row.get("forkers", 0) or 0)
        ratio_raw = row.get("ratio")
        ratio = None
        if ratio_raw not in (None, "", "null"):
            ratio = float(ratio_raw)

        recommendations.append({"repo": neighbor_repo, "count": stargazers})
        rich.append(
            {
                "repo": neighbor_repo,
                "stargazers": stargazers,
                "forkers": forkers,
                "ratio": ratio,
            }
        )

    if TOP_N is not None:
        recommendations = recommendations[:TOP_N]
        rich = rich[:TOP_N]

    return {"recommendations": recommendations, "clickhouse": rich}


def save_repo_file(repo: str, payload: Dict[str, List[Dict[str, object]]]) -> None:
    PER_REPO_DIR.mkdir(parents=True, exist_ok=True)
    safe = repo.replace("/", "__")
    body = {
        "repo": repo,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    (PER_REPO_DIR / f"{safe}.json").write_text(json.dumps(body, indent=2))


def save_results(username: str, results: List[Dict[str, object]]):
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "username": username,
        "results": results,
    }
    RECOMMENDATIONS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    snapshot = RECOMMENDATIONS_DIR / f"recommendations-{timestamp}.json"
    snapshot.write_text(json.dumps(payload, indent=2))
    LATEST_JSON.write_text(json.dumps(payload, indent=2))
    print(f"[INFO] Wrote {LATEST_JSON} and {snapshot}")


def process_repo(repo: str) -> Dict[str, object]:
    try:
        payload = fetch_repo_recommendations(repo)
        save_repo_file(repo, payload)
        print(f"[DONE] {repo} → {len(payload['recommendations'])} recommendations")
        return {"repo": repo, **payload}
    except ClickHouseError as exc:
        print(f"[ERROR] {repo}: {exc}")
        return {"repo": repo, "recommendations": [], "clickhouse": []}


def main():
    username = os.getenv("GH_USER") or os.getenv("GITHUB_USERNAME") or "SpreadSheets600"
    if not username:
        raise SystemExit("Set GH_USER or GITHUB_USERNAME in env")

    print(f"[INFO] Pulling starred repos for {username}")
    starred = fetch_user_stars(username)
    if not starred:
        print("No stars found in ClickHouse dataset.")
        return

    print(
        f"[INFO] Processing {len(starred)} repos (limit={RECENT_REPOS_LIMIT}) with {MAX_WORKERS} workers"
    )
    results: Dict[str, Dict[str, object]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(process_repo, repo): repo for repo in starred}
        for future in concurrent.futures.as_completed(future_map):
            repo = future_map[future]
            results[repo] = future.result()

    ordered_results = [results[repo] for repo in starred if repo in results]
    save_results(username, ordered_results)
    print("Done!")


if __name__ == "__main__":
    main()

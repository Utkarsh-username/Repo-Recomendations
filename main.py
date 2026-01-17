import os
import json
import yaml
import aiohttp
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse


load_dotenv()

GITHUB_API = "https://api.github.com"

STAR_SNAPSHOT_DIR = Path("data/starred")
USER_CACHE_DIR = Path("data/cache/users")
SETTINGS_PATH = Path("config/settings.yml")
STARGAZERS_CACHE_DIR = Path("data/cache/stargazers")

NEIGHBOR_FETCH_CHUNK = 10


def resolve_value(value):
    if isinstance(value, str):
        if value.startswith("${") and value.endswith("}"):
            var_name = value[2:-1]
            env_val = os.getenv(var_name)

            if env_val is None:
                print(f"Environment variable '{var_name}' not set")

            value = env_val
        if value.lower() in {"null", "none", ""}:
            return None

        if value.isdigit():
            return int(value)

    return value


def load_settings():
    if not SETTINGS_PATH.exists():
        print("[Settings] File not found.")
        exit(1)

    raw_data = yaml.safe_load(SETTINGS_PATH.read_text())
    if not raw_data:
        print("[Settings] File is empty.")
        exit(1)

    def resolve_dict(d):
        return {
            k: resolve_dict(v) if isinstance(v, dict) else resolve_value(v)
            for k, v in d.items()
        }

    config = resolve_dict(raw_data)

    if "username" not in config or config["username"] is None:
        print("Missing required config key: 'username'")

    if "use_pat" not in config:
        config["use_pat"] = True

    if not config["use_pat"]:
        config["token"] = None

    else:
        if "token" not in config or config["token"] is None:
            print("PAT token required when use_pat is true")

    print(f"[CONFIG LOADED] User: {config['username']}, Use PAT: {config['use_pat']}")
    return config


CONFIG = load_settings()
last_request_time = 0


def build_headers(token):
    h = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "repo-recommendations/0.2",
    }
    if token:
        h["Authorization"] = f"token {token}"

    return h


async def request_json(session, url, params=None):
    global last_request_time

    if not CONFIG.get("use_pat", True):
        current_time = asyncio.get_event_loop().time()
        time_since_last_request = current_time - last_request_time

        if time_since_last_request < 1.0:
            await asyncio.sleep(1.0 - time_since_last_request)

        last_request_time = asyncio.get_event_loop().time()

    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=30) as r:
                if r.status == 403:
                    reset = r.headers.get("X-RateLimit-Reset")

                    if reset:
                        sleep = max(
                            0, int(reset) - int(datetime.now(timezone.utc).timestamp())
                        )
                        print(f"Rate limit hit. Sleeping {sleep}s...")
                        await asyncio.sleep(sleep + 1)

                    return None

                if not r.ok:
                    return None

                return await r.json(content_type=None)

        except Exception:
            await asyncio.sleep(1.5 * (attempt + 1))

    return None


def extract_last(link):
    if not link:
        return 1

    for part in link.split(","):
        if 'rel="last"' in part:
            url = part.split(";")[0].strip()[1:-1]
            return int(parse_qs(urlparse(url).query)["page"][0])

    return 1


async def paginate(path, session):
    url = f"{GITHUB_API}{path}"

    async with session.get(url, params={"per_page": 100, "page": 1}) as first_resp:
        if not first_resp.ok:
            return []

        first = await first_resp.json()
        items = list(first)
        last = extract_last(first_resp.headers.get("Link", ""))

    async def fetch_page(p):
        data = await request_json(session, url, {"per_page": 100, "page": p})
        return data or []

    tasks = [fetch_page(i) for i in range(2, last + 1)]
    pages = await asyncio.gather(*tasks)

    for p in pages:
        items.extend(p)

    return items


async def fetch_user_starred(user, session):
    return await paginate(f"/users/{user}/starred", session)


def cache_user(login, data):
    USER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (USER_CACHE_DIR / f"{login}.json").write_text(json.dumps(data))


def load_user(login):
    p = USER_CACHE_DIR / f"{login}.json"

    if not p.exists():
        return None

    print(f"[Cache] Using cached stars for {login}")
    return json.loads(p.read_text())


def cache_stargazers(repo, data):
    STARGAZERS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (STARGAZERS_CACHE_DIR / f"{repo.replace('/', '__')}.json").write_text(
        json.dumps(data)
    )


def load_stargazers(repo):
    p = STARGAZERS_CACHE_DIR / f"{repo.replace('/', '__')}.json"

    if not p.exists():
        return None

    print(f"[Cache] Using cached stargazers for {repo}")
    return json.loads(p.read_text())


def clean_repo(repo):
    if repo.get("fork") and repo.get("stargazers_count", 0) < 50:
        return None

    if repo.get("stargazers_count", 0) < 5:
        return None

    return {
        "html_url": repo["html_url"],
        "full_name": repo["full_name"],
        "language": repo.get("language"),
        "description": repo.get("description"),
        "stargazers_count": repo.get("stargazers_count", 0),
    }


async def neighbors_overlap(repo, session, limits, username):
    cached = load_stargazers(repo["full_name"])

    if cached is None:
        stargazers = await paginate(f"/repos/{repo['full_name']}/stargazers", session)
        cache_stargazers(repo["full_name"], stargazers)

    else:
        stargazers = cached

    overlap = Counter()
    logins = []
    for u in stargazers:
        login = u.get("login")

        if login and login != username:
            logins.append(login)

        if (
            limits.get("max_neighbors_considered") is not None
            and len(logins) >= limits["max_neighbors_considered"]
        ):
            break

    for i in range(0, len(logins), NEIGHBOR_FETCH_CHUNK):
        batch = logins[i : i + NEIGHBOR_FETCH_CHUNK]

        async def fetch_stars(login):
            cached = load_user(login)

            if cached is not None:
                return cached

            raw = await fetch_user_starred(login, session)
            repos = [clean_repo(r) for r in raw if clean_repo(r)]

            if limits.get("stars_per_neighbor") is not None:
                repos = repos[: limits["stars_per_neighbor"]]

            cache_user(login, repos)
            return repos

        results = await asyncio.gather(*[fetch_stars(l) for l in batch])
        for repos in results:
            overlap.update(
                r["full_name"] for r in repos if r["full_name"] != repo["full_name"]
            )

    return overlap


async def main():
    username = CONFIG["username"]
    limits = CONFIG.get("limits", {})
    max_workers = CONFIG.get("max_workers", 2)

    print(
        f"\n[START RUN] Processing for user: {username}, Use PAT: {CONFIG['use_pat']}"
    )

    headers = build_headers(CONFIG["token"])
    async with aiohttp.ClientSession(headers=headers) as session:
        base = await fetch_user_starred(username, session)
        base = [clean_repo(r) for r in base if clean_repo(r)]

        repos_to_process = limits.get("repos_to_process")
        if repos_to_process is not None and repos_to_process > 0:
            base = base[:repos_to_process]

        print(f"[INFO] Total repos to analyze: {len(base)}")

        sem = asyncio.Semaphore(max_workers)

        async def process(repo):
            async with sem:
                overlap = await neighbors_overlap(repo, session, limits, username)
                recs = [
                    r
                    for r, c in overlap.most_common()
                    if c >= limits.get("min_cooccurrence", 1)
                ]
                top_n = limits.get("top_n")
                if top_n is not None:
                    recs = recs[:top_n]
                print(f"Processed {repo['full_name']} → {len(recs)} recommendations")
                return repo["full_name"], recs

        done = await asyncio.gather(*[process(r) for r in base])
        print(f"\n[COMPLETE] Processed {len(done)} repos")

        results = [{"repo": r, "recommendations": recs} for r, recs in done]
        print("\n[FINAL OUTPUT]")
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

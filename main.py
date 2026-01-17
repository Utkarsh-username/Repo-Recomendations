import os
import time
import yaml
import json
import copy
import requests
from dotenv import load_dotenv
from collections import Counter
from datetime import datetime, timezone


DEFAULT_CONFIG_PATH = "config/settings.yml"

DEFAULT_CONFIG = {
    "auth": {"env_var": "GH_TOKEN", "token": None},
    "user": {"login": None},
    "limits": {
        "stargazers_per_repo": 50,
        "stars_per_neighbor": 75,
        "max_user_stars": None,
        "min_cooccurrence": 2,
        "request_delay_ms": 0,
        "top_n": 10,
    },
    "output": {
        "directory": "data/recommendations",
        "filename": "recommendations.json",
        "append_timestamp": True,
    },
}

load_dotenv()


def load_config(path=None):
    config_path = path or DEFAULT_CONFIG_PATH

    if not os.path.exists(config_path):
        print(f"[config] Config file not found at {config_path}")
        return None

    with open(config_path, "r", encoding="utf-8") as handle:
        try:
            user_config = yaml.safe_load(handle) or {}

        except Exception as e:
            print(f"[config] Error parsing config file: {e}")
            return None

    merged = copy.deepcopy(DEFAULT_CONFIG)

    for key, value in user_config.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value

    if not merged.get("user", {}).get("login"):
        print("[config] GitHub username must be specified in config under 'user.login'")
        return None

    print(f"[config] Loaded configuration for user {merged['user']['login']}")
    return merged


def resolve_token(auth_config):
    token = auth_config.get("token")

    if token:
        return token

    env_var = auth_config.get("env_var") or "GH_TOKEN"
    return os.getenv(env_var)


def build_headers(token):
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "repo-recommendations/0.1",
    }

    if token:
        headers["Authorization"] = f"token {token}"

    return headers


def request_json(url, headers, params=None, request_delay=0.0, return_response=False):
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)

    except Exception as e:
        print(f"[http] request error for {url} : {e}")

        if request_delay:
            time.sleep(request_delay)

        return None

    if request_delay:
        time.sleep(request_delay)

    if response.status_code == 403:
        print(f"[http] rate limit on : {url}")
        return None

    if response.status_code == 404:
        print(f"[http] resource not found on : {url}")
        return None

    if not response.ok:
        preview = response.text[:200].replace("\n", " ")
        print(f"[http] unexpected {response.status_code} for {url} : {preview}")

        return None

    payload = response.json()

    if return_response:
        return payload, response

    return payload


def paginate_api(path, limit, headers, request_delay, per_page=100):
    if limit is not None and limit <= 0:
        limit = None

    items = []
    per_page = per_page if not limit else min(per_page, limit)

    next_url = f"https://api.github.com{path}"
    params = {"per_page": per_page, "page": 1}
    page = 1

    while next_url:
        if limit and len(items) >= limit:
            break

        result = request_json(
            next_url,
            headers,
            params=params,
            request_delay=request_delay,
            return_response=True,
        )

        if not result:
            print(f"[paginate] stopping at page {page} for {path} (no data)")
            break

        data, response = result

        params = None

        items.extend(data)

        if limit and len(items) >= limit:
            break

        next_url = None
        link_header = response.headers.get("Link", "")
        for part in link_header.split(","):
            section = part.strip().split(";")
            if len(section) < 2:
                continue
            url_part = section[0].strip()
            rels = section[1:]
            if any('rel="next"' in rel for rel in rels):
                if url_part.startswith("<") and url_part.endswith(">"):
                    next_url = url_part[1:-1]
                else:
                    next_url = url_part
                break

        if not next_url:
            print(f"[paginate] reached final page {page} for {path}")
            break

        page += 1

    return items[:limit] if limit else items


def fetch_user_starred(username, limit, headers, request_delay):
    return paginate_api(f"/users/{username}/starred", limit, headers, request_delay)


def fetch_repo_stargazers(repo_full_name, limit, headers, request_delay):
    return paginate_api(
        f"/repos/{repo_full_name}/stargazers", limit, headers, request_delay
    )


def extract_repo_metadata(repo):
    full_name = repo.get("full_name")

    if not full_name:
        return None

    owner = repo.get("owner", {}) or {}

    return {
        "full_name": full_name,
        "name": repo.get("name"),
        "owner": owner.get("login"),
        "language": repo.get("language"),
        "html_url": repo.get("html_url"),
        "description": repo.get("description"),
        "stargazers_count": repo.get("stargazers_count", 0),
    }


def get_recommendations(config):
    username = config["user"]["login"]
    limits = config["limits"]

    auth_config = config.get("auth", {})
    token = resolve_token(auth_config)

    request_delay = limits.get("request_delay_ms", 0) / 1000.0

    headers = build_headers(token)

    print(f"[build] Collecting starred repos for {username}")

    max_user_stars = limits.get("max_user_stars")
    print(
        f"[build] Using max_user_stars={max_user_stars if max_user_stars is not None else 'ALL'}"
    )

    base_repos_raw = fetch_user_starred(
        username, max_user_stars, headers, request_delay
    )
    print(
        f"[build] Retrieved {len(base_repos_raw)} starred repos"
        + (" (limited)" if max_user_stars else "")
    )

    base_repos = []
    repo_metadata = {}

    for repo in base_repos_raw:
        meta = extract_repo_metadata(repo)

        if meta:
            base_repos.append(meta)
            repo_metadata[meta["full_name"]] = meta

    results = []
    unique_recommendations = set()

    for index, repo_meta in enumerate(base_repos, start=1):
        print(f"[build] ({index}/{len(base_repos)}) {repo_meta['full_name']}")

        repo_name = repo_meta["full_name"]
        stargazers = fetch_repo_stargazers(
            repo_name, limits.get("stargazers_per_repo"), headers, request_delay
        )
        print(f"[build] Repo {repo_name}: fetched {len(stargazers)} stargazers")

        overlap = Counter()
        neighbors_with_data = 0

        for user in stargazers:
            login = user.get("login")
            if not login or login == username:
                continue

            neighbor_stars_raw = fetch_user_starred(
                login, limits.get("stars_per_neighbor"), headers, request_delay
            )
            neighbor_stars = []

            for repo in neighbor_stars_raw:
                meta = extract_repo_metadata(repo)

                if meta:
                    neighbor_stars.append(meta)
                    repo_metadata.setdefault(meta["full_name"], meta)

            if not neighbor_stars:
                continue

            neighbors_with_data += 1
            overlap.update(
                repo["full_name"]
                for repo in neighbor_stars
                if repo["full_name"] != repo_name
            )

        recommendations = select_recommendations(
            overlap, limits, repo_metadata, unique_recommendations
        )
        print(
            f"[build] Repo {repo_name} -> {len(recommendations)} recommendations (neighbors {neighbors_with_data})"
        )

        result = {
            "source": repo_meta,
            "stargazers_sampled": len(stargazers),
            "neighbors_with_public_stars": neighbors_with_data,
            "recommendations": recommendations,
        }
        results.append(result)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "user": username,
        "stats": {
            "source_repos_processed": len(base_repos),
            "unique_recommendations": len(unique_recommendations),
            "stars_per_neighbor": limits.get("stars_per_neighbor"),
            "stargazers_per_repo": limits.get("stargazers_per_repo"),
        },
        "results": results,
    }

    return payload


def select_recommendations(overlap, limits, repo_metadata, unique_recommendations):
    selected = []

    if not overlap:
        return selected

    for repo_name, count in overlap.most_common():
        if count < limits["min_cooccurrence"]:
            break

        metadata = repo_metadata.get(repo_name)
        if not metadata:
            continue

        selected.append(
            {
                "full_name": metadata["full_name"],
                "html_url": metadata["html_url"],
                "description": metadata["description"],
                "language": metadata["language"],
                "stargazers_count": metadata["stargazers_count"],
                "overlap_count": count,
            }
        )

        unique_recommendations.add(metadata["full_name"])

        if len(selected) >= limits["top_n"]:
            break

    return selected


def get_output(output_config):
    directory = output_config["directory"]
    filename = output_config["filename"]

    if output_config.get("append_timestamp", True):
        stem, ext = os.path.splitext(filename)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{stem}-{timestamp}{ext or '.json'}"

    os.makedirs(directory, exist_ok=True)
    return os.path.join(directory, filename)


def save_json(data, filepath):
    with open(filepath, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)


def main(config):
    token = resolve_token(config.get("auth", {}))

    if not token:
        print("[config] No token found; requests will be heavily rate limited.")

    limits_conf = config["limits"]

    request_delay_ms = limits_conf.get("request_delay_ms", 0)

    if not token and request_delay_ms == 0:
        request_delay_ms = 1000

    config["limits"]["request_delay_ms"] = request_delay_ms

    payload = get_recommendations(config)

    print(
        f"[summary] Processed {payload['stats']['source_repos_processed']} repos; "
        f"unique recommendations: {payload['stats']['unique_recommendations']}"
    )

    output_path = get_output(config["output"])
    save_json(payload, output_path)

    print(f"[done] Saved recommendations to {output_path}")


if __name__ == "__main__":
    config = load_config()
    
    main(config)

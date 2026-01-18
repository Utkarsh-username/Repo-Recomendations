# Repo Recommendations

Generate GitHub repo recommendations from your own stars and co-stargazers. Outputs are JSON files under `data/recommendations/`.

## Config (config/settings.yml)

- `auth.env_var` / `auth.token`: where your PAT is read from (env wins if both). Needs `public_repo` scope for rate limits; `repo` if private stars.
- `user.login`: GitHub username whose stars are analyzed.
- `limits.max_user_stars`: cap on how many of your starred repos to fetch (set `null` for all).
- `limits.stargazers_per_repo`: how many stargazers to sample per repo (set `null` for all pages).
- `limits.stars_per_neighbor`: how many stars to fetch per co-stargazer (set `null` for all pages).
- `limits.min_cooccurrence`: minimum overlapping stars before a repo is recommended.
- `limits.top_n`: max recommendations per source repo.
- `limits.request_delay_ms`: optional delay between requests; auto-increases when no token is present.
- `output.directory` / `output.filename`: where the recommendation JSON is written; timestamp is appended when `append_timestamp` is true.

## Automation

- Last recommendations run: <!-- RECO_TS_START -->2026-01-18 11:48:26 UTC<!-- RECO_TS_END -->
- Latest recommendations file: <!-- RECO_FILE_START -->[data/recommendations/latest.json](data/recommendations/latest.json)<!-- RECO_FILE_END -->
- Last Pages deploy: <!-- PAGES_TS_START -->not yet run<!-- PAGES_TS_END -->
- Pages source JSON: <!-- PAGES_JSON_START -->n/a<!-- PAGES_JSON_END -->

# Repo Recommendations

Generate GitHub repository recommendations based on your starred repositories and co-stargazers. The tool analyzes your GitHub stars and identifies other repositories that users who starred the same projects also found interesting. Results are saved as JSON files in the `data/recommendations/` directory.

## Configuration

All configuration is managed through `config/settings.yml`. You can override any setting with environment variables.

### Settings Overview

#### ClickHouse Configuration

- `clickhouse.url`: Database URL (default: `https://play.clickhouse.com`)
- `clickhouse.table`: Table containing GitHub events (default: `github_events`)
- `clickhouse.timeout`: Request timeout in seconds (default: `60`)

#### Processing Limits

- `processing.recent_repos_limit`: Maximum number of starred repositories to analyze (set `null` for no limit, default: `null`)
- `processing.max_workers`: Number of parallel workers for processing (default: `4`)
- `processing.top_n`: Maximum recommendations per repository (default: `10`)

#### Paths

- `paths.recommendations_dir`: Directory for storing recommendation files (default: `data/recommendations`)
- `paths.latest_json`: Path to the latest recommendations file (default: `data/recommendations/latest.json`)

#### User

- `user.login`: GitHub username to analyze

## Usage

Run the application with:

```bash
python main.py
```

The script will:

1. Fetch your starred repositories
2. Find repositories that share stargazers with your starred repos
3. Generate recommendations based on co-stargazing patterns
4. Save results to `data/recommendations/latest.json`
5. Render a static `index.html` using the Jinja template in `templates/index.html`

## Recommendation Data Fields

Each recommended repository now includes the following information:

- repo: The repository name (e.g., "user/repo-name")
- count: Number of overlapping stargazers with your starred repo
- total_stars: Total GitHub stars for the recommended repo
- total_forks: Total forks for the recommended repo
- score: Overlap ratio (`count / total_stars`, 0 if no stars)

## Automation

- Last recommendations run: <!-- RECO_TS_START -->2026-01-18 17:55:05 UTC<!-- RECO_TS_END -->
- Latest recommendations file: <!-- RECO_FILE_START -->[recommendations/latest.json](recommendations/latest.json)<!-- RECO_FILE_END -->
- Last Pages deploy: <!-- PAGES_TS_START -->not yet run<!-- PAGES_TS_END -->
- Pages source JSON: <!-- PAGES_JSON_START -->n/a<!-- PAGES_JSON_END -->

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

- `processing.recent_repos_limit`: Maximum number of starred repositories to analyze (set `null` for no limit, default: `10`)
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

## Recommendation Data Fields

Each recommended repository now includes the following information:

- `repo`: The repository name (e.g., "user/repo-name")
- `count`: Number of your stargazers that also starred this repository
- `total_stars`: Total number of stars for this repository
- `first_event_date`: Date of the first recorded event for this repository
- `last_event_date`: Date of the most recent recorded event for this repository
- `push_events`: Count of push events for this repository
- `comment_events`: Count of issue comment events for this repository
- `issue_events`: Count of issue events for this repository
- `pr_events`: Count of pull request events for this repository
- `fork_events`: Count of fork events for this repository
- `watch_events`: Count of watch/star events for this repository
- `total_events`: Total number of events recorded for this repository

## Automation

- Last recommendations run: <!-- RECO_TS_START -->2026-01-18 17:28:46 UTC<!-- RECO_TS_END -->
- Latest recommendations file: <!-- RECO_FILE_START -->[recommendations/latest.json](recommendations/latest.json)<!-- RECO_FILE_END -->
- Last Pages deploy: <!-- PAGES_TS_START -->not yet run<!-- PAGES_TS_END -->
- Pages source JSON: <!-- PAGES_JSON_START -->n/a<!-- PAGES_JSON_END -->

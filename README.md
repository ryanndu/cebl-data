# cebl-data

Scrapes Canadian Elite Basketball League game data and publishes it as
Parquet files on GitHub Releases. The [ceblpy](https://github.com/ryanndu/ceblpy)
and [ceblR](https://github.com/ryanndu/ceblR) packages read these files so if
you want the data in Python or R, start there. This repository is the
pipeline that produces them.

## Datasets

Every dataset is one Parquet file, updated on each run.

| dataset | release tag | asset | grain |
|---|---|---|---|
| Schedule | `schedule` | `cebl_schedule.parquet` | one per game |
| Team box score | `team-boxscore` | `cebl_team_boxscore.parquet` | one per team per game |
| Player box score | `player-boxscore` | `cebl_player_boxscore.parquet` | one per player per game |
| Play-by-play | `pbp` | `cebl_pbp.parquet` | one per event |

Read one directly:

```python
import pandas as pd

url = "https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.parquet"
schedule = pd.read_parquet(url)
```

Column descriptions live in the ceblpy and ceblR documentation rather than being
duplicated here.

## Sources

Two sources:

- **CEBL** (`api.data.cebl.ca`) returns one season's schedule data per call.
- **Genius Sports livestats** returns one game's data per call: box
  scores, play-by-play, shot coordinates. The schedule's stats URL contains
  the id needed to fetch it.

## How the pipeline works

`pipeline.py` compares what is published against what the sources say, and
fetches only the difference. The two halves follow different rules.

**The schedule replaces a season.** Rows for the requested seasons are
dropped and refetched, so scores and statuses that changed after
publication are corrected.

**Game datasets append.** Only games missing from the published file are
fetched, since a box score doesn't change once the game is over.

Both read the published asset first. A release with no asset yet reads back
as an empty frame, so nothing matches and every game counts as new, which
means a backfill and a daily update are the same code path, and there is no
separate initialize step.

## Schemas

Each dataset has a JSON config in `src/cebl_data/configs/` with three keys:

- `rename` — source field to published column
- `dropped` — source fields deliberately not published
- `dtypes` — every published column and its type, in order

Between them they account for every field the source returns, so anything
unrecognised gets reported when a build runs instead of passing silently.

`dtypes` also selects and orders the columns. A column missing from one
game's payload comes back null rather than absent, so every game produces
the same shape.

**Changing a schema means republishing from scratch.** Missing rows get
backfilled on the next run; missing columns don't. Adding a column to an
existing dataset means deleting the release asset and rebuilding.

## Running it

Set up with [uv](https://docs.astral.sh/uv/), which installs the exact
versions in `uv.lock`:

```bash
uv sync
```

Publishing needs a token with write access to this repository's contents:

```
GITHUB_TOKEN=...
GITHUB_REPOSITORY=ryanndu/cebl-data
```

Put those in `.env` for local runs. In GitHub Actions both are provided
automatically.

Update the current season:

```bash
uv run python -m cebl_data.pipeline
```

Rebuild every season:

```bash
uv run python -c "
from dotenv import load_dotenv
from cebl_data.pipeline import update_all
load_dotenv()
update_all(list(range(2019, 2027)))
"
```

Checks:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Automation

`update.yml` runs daily at 08:05 UTC from May through August, and can be
triggered by hand from the Actions tab. `ci.yml` runs lint and tests on
pushes to main and on pull requests. `keepalive.yml` exists because GitHub
disables scheduled workflows in repositories with no activity for 60 days.
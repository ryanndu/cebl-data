import io
import os

import pandas as pd
import requests
from github import Github

from cebl_data.utils import load_packaged_config


def asset_name(dataset: str) -> str:
    """Returns the published filename for a dataset.

    Args:
        dataset (str): A dataset name, which is also its release tag.

    Returns:
        str: The asset filename, e.g. ``cebl_team_boxscore.parquet``.
    """
    return f"cebl_{dataset.replace('-', '_')}.parquet"


def config_name(dataset: str) -> str:
    """Returns the config filename for a dataset.

    Args:
        dataset (str): A dataset name, which is also its release tag.

    Returns:
        str: The config filename, e.g. ``team_boxscore.json``.
    """
    return f"{dataset.replace('-', '_')}.json"


def read_published(dataset: str) -> pd.DataFrame:
    """Reads a published dataset from its release asset.

    A dataset that has never been published yields an empty frame with the
    right columns and dtypes, so callers can anti-join against it without a
    cold-start branch.

    Args:
        dataset (str): A dataset name, which is also its release tag.

    Returns:
        pd.DataFrame: The published rows, or an empty frame shaped like them.
    """
    config = load_packaged_config(config_name(dataset))
    url = "https://github.com/{repository}/releases/download/{tag}/{asset}".format(
        repository=os.environ["GITHUB_REPOSITORY"],
        tag=dataset,
        asset=asset_name(dataset),
    )

    response = requests.get(url, timeout=60)
    if response.status_code == 404:
        return pd.DataFrame(columns=list(config["dtypes"])).astype(config["dtypes"])
    response.raise_for_status()
    return pd.read_parquet(io.BytesIO(response.content))


def write_published(frame: pd.DataFrame, dataset: str) -> None:
    """Replaces a dataset's release asset with the given frame.

    GitHub has no way to overwrite an asset, so the old one is deleted first.

    Args:
        frame (pd.DataFrame): The full dataset to publish.
        dataset (str): A dataset name, which is also its release tag.
    """
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)

    repository = Github(os.environ["GITHUB_TOKEN"]).get_repo(
        os.environ["GITHUB_REPOSITORY"]
    )
    release = repository.get_release(dataset)

    for asset in release.get_assets():
        if asset.name == asset_name(dataset):
            asset.delete_asset()

    release.upload_asset_from_memory(
        buffer.getvalue(), len(buffer.getvalue()), name=asset_name(dataset)
    )

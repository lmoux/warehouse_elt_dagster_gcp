import os

import requests


def download_file_locally(
    url: str,
    download_dir: str,
    preferred_name: str | None = None,
    overwrite: bool = False,
):
    """Attempts the download of a URL into a file system that is locally addressable."""
    os.makedirs(download_dir, exist_ok=True)
    local_file_name = (
        preferred_name if preferred_name is not None else os.path.basename(url)
    )
    local_path = os.path.join(download_dir, local_file_name)

    if not overwrite and os.path.exists(local_path):
        return None

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_path

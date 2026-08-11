"""
Download Semantic Scholar Academic Graph (S2AG) datasets (e.g. "papers",
"citations", "s2orc", ...) from the Semantic Scholar bulk-datasets API into
S2AG_DIRE/<RELEASE_ID>/<dataset_name>/, where RELEASE_ID is the release date
(e.g. 2026-07-28) so several releases can coexist side by side. For each dataset
it lists the file shards and downloads them one after another (skipping any
already present, so it is resumable).

Provide your own API key in the SEMANTIC_SCHOLAR_API_KEY environment variable
(see meta_config.py for how to set it), and list
the datasets you want in DATASET_NAMES; the downstream pipeline needs "papers"
(get_s2ag_meta.py), "citations" (get_s2ag_citation.py), and "s2orc"
(full_text_analysis/get_s2orc_meta.py).
"""

import os
from datetime import datetime
import pickle
import sys
from urllib.parse import urlparse

import requests
from tqdm import tqdm

# utils.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
from utils import (FINISHED_MARKER, S2AG_DIRE,  # noqa: E402
                   get_release_id, require_semantic_scholar_api_key)

# Semantic Scholar API key, read from the SEMANTIC_SCHOLAR_API_KEY environment
# variable. Fail here with an explanation rather than sending an unauthenticated
# request and hitting an opaque 403 halfway through a download.
API_KEY = require_semantic_scholar_api_key()

# which S2AG datasets to download, in order. Options include:
#   papers, citations, authors, abstracts, tldrs, embeddings-specter_v2, s2orc
# The downstream pipeline needs "papers", "citations" and "s2orc".
DATASET_NAMES = ["papers", "citations", "authors"]
print("DATASET_NAMES", DATASET_NAMES)

# 1. release to download (RELEASE_ID in utils.py, or the latest release)
RELEASE_ID = get_release_id()
print(f"Release ID: {RELEASE_ID}")

CHUNK_SIZE = 1 << 20  # 1 MiB per read while streaming a shard to disk


def download_file(url, save_path, position):
    """Stream one shard to save_path with a byte-level progress bar.

    Downloads to a .part file first and renames on success, so an interrupted
    run never leaves a truncated file that the skip-if-exists check would
    mistake for a finished download.
    """
    part_path = save_path + '.part'
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        total = int(response.headers.get('Content-Length', 0)) or None
        with open(part_path, 'wb') as f, tqdm(
            total=total, unit='B', unit_scale=True, unit_divisor=1024,
            desc=os.path.basename(save_path)[:40], position=position,
            leave=False,
        ) as bar:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                bar.update(len(chunk))
    os.replace(part_path, save_path)


# progress bars, outermost first:
#   position 0  datasets
#   position 1  shards within the current dataset
#   position 2  bytes within the current shard
for dataset_name in tqdm(DATASET_NAMES, desc='datasets', position=0):
    # everything lands in S2AG_DIRE (raw_data/), under the release date
    local_path = os.path.join(S2AG_DIRE, RELEASE_ID, dataset_name)
    os.makedirs(local_path, exist_ok=True)
    tqdm.write(f"\n{dataset_name}: saving to {local_path}")

    # 2. dataset metadata
    response = requests.get(
        f"https://api.semanticscholar.org/datasets/v1/release/{RELEASE_ID}/dataset/{dataset_name}/",
        headers={"x-api-key": API_KEY}
    ).json()

    for url in tqdm(response["files"], desc=f'{dataset_name} shards',
                    position=1, leave=False):
        filename = os.path.basename(urlparse(url).path)
        save_path = os.path.join(local_path, filename)

        if os.path.exists(save_path):
            # already downloaded, skip
            continue

        try:
            download_file(url, save_path, position=2)
        except Exception as e:
            tqdm.write(f"\nError downloading {url}: {e}")
            raise

    # Only reached once every shard of this dataset is on disk, since any failure
    # above re-raises. get_local_release_id() treats a release as usable only when
    # all of its dataset directories carry this marker, so an interrupted download
    # is never processed as though it were complete.
    with open(os.path.join(local_path, FINISHED_MARKER), 'wb') as f:
        pickle.dump({'release_id': RELEASE_ID,
                     'dataset_name': dataset_name,
                     'file_count': len(response["files"]),
                     # when the data actually came down, which is what the site
                     # shows; the release id is the release date, not this
                     'downloaded_at': datetime.now().isoformat(timespec='seconds')},
                    f, pickle.HIGHEST_PROTOCOL)
    tqdm.write(f"{dataset_name}: complete ({len(response['files'])} shards)")

print("\nDownloaded all shards of all datasets (skipped existing files).")

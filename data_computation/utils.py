"""Shared helpers. Every setting lives in meta_config.py; this re-exports the
paths and the release resolver so existing imports keep working.
"""

import requests
from tqdm import tqdm
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

from meta_config import (  # noqa: F401  (re-exported for the steps that import them)
    FINISHED_MARKER,
    META_DIRE,
    RELEASE_ID,
    S2AG_DIRE,
    SEMANTIC_SCHOLAR_API_KEY,
    get_local_release_id,
    require_semantic_scholar_api_key,
)


def get_release_id():
    """RELEASE_ID if set, otherwise the latest release id from the API (needs network)."""
    if RELEASE_ID:
        return RELEASE_ID
    response = requests.get(
        "https://api.semanticscholar.org/datasets/v1/release/latest"
    ).json()
    return response["release_id"]


def process_shards(worker, file_dire, on_result, desc, max_workers, batch_count,
                   skip_batch=None, on_batch=None):
    """Run worker over every .gz shard in file_dire, passing each result to on_result.

    Shards are dealt round-robin into batch_count batches; each batch gets its own
    process pool, so results are merged as they arrive rather than all being held
    until the end. max_workers of 1 skips the pool and runs in-process.

    worker must be a module-level function (it is pickled to the subprocesses);
    on_result runs in the parent, so it is where shared state gets merged.

    The batch hooks are what a resumable caller needs, and both are optional:
    skip_batch(batch, files) is asked before any work is done and skips the batch
    when it returns True; on_batch(batch, files) runs once every shard of a batch
    has been through on_result, which is the only moment a batch is known complete
    and so the only safe moment to check it out to disk.

    Returns batch -> [file names], the full mapping including skipped batches, so
    a caller reassembling checkpoints can tell what each one was supposed to hold.
    """
    file_list = sorted(f for f in os.listdir(file_dire) if f.endswith('.gz'))
    if not file_list:
        raise SystemExit(f'no .gz shards found in {file_dire}. Run download_s2ag.py first.')
    print(f"{desc}: {len(file_list)} shards, batch_count {batch_count}, workers {max_workers}")

    batch2file = defaultdict(list)
    for i, file in enumerate(file_list):
        batch2file[i % batch_count].append(file)

    for batch, files in tqdm(batch2file.items(), desc=f"{desc} batches"):
        if skip_batch is not None and skip_batch(batch, files):
            continue
        if max_workers > 1:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(worker, os.path.join(file_dire, file))
                    for file in files
                ]
                for future in tqdm(as_completed(futures), total=len(futures),
                                   desc=f"{desc} batch {batch}", leave=False):
                    on_result(future.result())
        else:
            for file in files:
                on_result(worker(os.path.join(file_dire, file)))
        if on_batch is not None:
            on_batch(batch, files)

    return dict(batch2file)

"""
Fetch the text needed to judge what a faculty paper is ABOUT, from the Semantic
Scholar API.

The bulk dumps carry titles, and a title alone is a thin basis for deciding which
of a researcher's areas a paper belongs to. This pulls the abstract, the TLDR and
S2's own field-of-study tags for the papers in the faculty paper list --
get_paper_category_s2.py then classifies from them.

    python get_paper_category_s1.py
    python get_paper_category_s1.py --batch-size 200 --sleep 1.5

Only the papers in faculty_paper_list_w{first}_{last}.xlsx are fetched, and each
corpusid ONCE however many rostered authors share it. That is a few hundred
thousand at most, which is why this goes to the API rather than downloading the
"abstracts" dump: the dump is the whole corpus, hundreds of gigabytes, to answer
a question about a rounding error's worth of it.

RESUMABLE, and built to be. The API is rate limited, so a full fetch is measured
in hours; every batch is written to the cache before the next is asked for, and a
rerun asks only for what is missing. Interrupting it costs the batch in flight.
Deleting the cache file is how you force a refetch.

A paper the API does not know, or knows without an abstract, is cached as an
EMPTY record rather than left out. The distinction that matters on a rerun is
"asked and got nothing" versus "not asked yet", and without the empty record
every rerun would ask again for the same permanently-missing papers.

Needs SEMANTIC_SCHOLAR_API_KEY. Without a key the endpoint is throttled hard
enough that a fetch this size is not practical.

Input  : META_DIRE/<release_id>/faculty_paper_list_w{first}_{last}.xlsx
                                                   (get_faculty_paper_list.py)
Output : META_DIRE/<release_id>/cid2content.pkl
             corpusid -> {'abstract', 'tldr', 'fields', 'title'}
"""

import argparse
import os
import pickle
import sys
import time

import pandas as pd
import requests
from tqdm import tqdm

# meta_config.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import meta_config as cfg  # noqa: E402
from meta_config import (META_DIRE, get_local_release_id,  # noqa: E402
                         require_semantic_scholar_api_key)

OUTPUT_NAME = 'cid2content.pkl'

BATCH_URL = 'https://api.semanticscholar.org/graph/v1/paper/batch'
FIELDS = 'title,abstract,tldr,s2FieldsOfStudy'

# The endpoint's documented ceiling. Asking for more is rejected outright, so
# this is a limit rather than a tuning knob.
MAX_BATCH = 500

# Seconds between requests. The published limit for an authenticated key is one
# request a second; a little over that is cheap insurance against the 429s that
# cost a retry each.
SLEEP = 1.1

# A 429 or a 5xx is worth waiting out -- the whole fetch is long and restarting
# it is expensive -- but not forever, since a persistent failure means something
# is wrong rather than busy.
MAX_RETRIES = 5
RETRY_BACKOFF = 5.0


def load_cache(path):
    if not os.path.isfile(path):
        return {}
    with open(path, 'rb') as f:
        cache = pickle.load(f)
    print(f"cache: {len(cache)} papers already fetched from {os.path.basename(path)}")
    return cache


def save_cache(path, cache):
    """Atomically, because this is written after every batch.

    A kill during the write would otherwise leave a truncated pickle where the
    only copy of several hours of fetching used to be.
    """
    tmp_path = path + '.tmp'
    with open(tmp_path, 'wb') as f:
        pickle.dump(cache, f, pickle.HIGHEST_PROTOCOL)
    os.replace(tmp_path, path)


def fetch_batch(session, api_key, cids):
    """The API's answer for one batch, as a list aligned with cids.

    Returns None for a paper the API does not know. Raises after MAX_RETRIES.
    """
    payload = {'ids': [f'CorpusId:{cid}' for cid in cids]}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(BATCH_URL, params={'fields': FIELDS},
                                    json=payload,
                                    headers={'x-api-key': api_key}, timeout=120)
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise SystemExit(f'the API was unreachable {MAX_RETRIES} times '
                                 f'in a row: {exc}\nWhat has been fetched is '
                                 f'already saved; rerun to carry on.')
            time.sleep(RETRY_BACKOFF * attempt)
            continue

        if response.status_code == 200:
            return response.json()

        # 429 is the rate limiter, 5xx is the service; both pass
        if response.status_code == 429 or response.status_code >= 500:
            if attempt == MAX_RETRIES:
                raise SystemExit(
                    f'the API returned {response.status_code} {MAX_RETRIES} '
                    f'times in a row.\nWhat has been fetched is already saved; '
                    f'rerun to carry on, or raise --sleep.')
            time.sleep(RETRY_BACKOFF * attempt)
            continue

        # anything else is our fault and retrying will not fix it
        raise SystemExit(f'the API returned {response.status_code}: '
                         f'{response.text[:400]}')


def record_from(paper):
    """One cached record. An unknown paper still gets a record -- an empty one.

    s2FieldsOfStudy arrives as [{'category': 'Computer Science', 'source':
    's2-fos-model'}, ...]. Only the categories are kept, deduplicated, order
    preserved: the source says how S2 arrived at the tag, which is not something
    a classifier downstream can use.
    """
    if paper is None:
        return {'title': '', 'abstract': '', 'tldr': '', 'fields': []}

    tldr = paper.get('tldr') or {}
    fields = []
    for entry in paper.get('s2FieldsOfStudy') or []:
        category = (entry or {}).get('category')
        if category and category not in fields:
            fields.append(category)

    return {
        'title': (paper.get('title') or '').strip(),
        'abstract': (paper.get('abstract') or '').strip(),
        'tldr': (tldr.get('text') or '').strip(),
        'fields': fields,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--batch-size', type=int, default=MAX_BATCH,
                    help=f'papers per request (default and maximum {MAX_BATCH})')
    ap.add_argument('--sleep', type=float, default=SLEEP,
                    help=f'seconds between requests (default {SLEEP})')
    ap.add_argument('--limit', type=int, default=0, metavar='N',
                    help='stop after N papers. For trying the thing out before '
                         'committing hours to it')
    args = ap.parse_args()

    if not 1 <= args.batch_size <= MAX_BATCH:
        raise SystemExit(f'--batch-size must be between 1 and {MAX_BATCH}')

    api_key = require_semantic_scholar_api_key()

    release_id = get_local_release_id(META_DIRE, require_finished=False)
    cfg.print_config(release_id)
    meta_dire = os.path.join(META_DIRE, release_id)

    sheet_path = os.path.join(
        meta_dire, cfg.faculty_paper_list_file_base() + '.xlsx')
    if not os.path.isfile(sheet_path):
        raise SystemExit(f'{sheet_path} does not exist. '
                         f'Run get_faculty_paper_list.py first.')

    # one row per (person, paper), so the same paper appears once per rostered
    # co-author; the API should be asked about it once
    sheet = pd.read_excel(sheet_path, usecols=['corpusid'])
    cids = sorted(set(int(c) for c in sheet['corpusid']))
    print(f"{len(sheet)} rows in the sheet, {len(cids)} distinct papers")

    cache_path = os.path.join(meta_dire, OUTPUT_NAME)
    cache = load_cache(cache_path)

    missing = [cid for cid in cids if cid not in cache]
    if args.limit:
        missing = missing[:args.limit]
        print(f"--limit {args.limit}: stopping after that many")
    if not missing:
        print("nothing to fetch; every paper in the sheet is already cached")
        print("finished")
        return

    n_batches = (len(missing) + args.batch_size - 1) // args.batch_size
    print(f"{len(missing)} to fetch, {n_batches} requests, "
          f"about {n_batches * args.sleep / 60:.0f} minutes at "
          f"--sleep {args.sleep}")

    session = requests.Session()
    n_unknown = 0
    n_with_abstract = 0
    try:
        for start in tqdm(range(0, len(missing), args.batch_size),
                          total=n_batches, desc='fetch'):
            batch = missing[start:start + args.batch_size]
            papers = fetch_batch(session, api_key, batch)

            # the response is positional: entry i answers for batch[i], and is
            # null for a corpusid the API does not know
            for cid, paper in zip(batch, papers):
                record = record_from(paper)
                cache[cid] = record
                if paper is None:
                    n_unknown += 1
                elif record['abstract']:
                    n_with_abstract += 1

            # before the next request, not after the loop: this is the whole
            # resumability story and a batch is cheap to write
            save_cache(cache_path, cache)
            time.sleep(args.sleep)
    except KeyboardInterrupt:
        save_cache(cache_path, cache)
        raise SystemExit(f'\ninterrupted; {len(cache)} papers are cached. '
                         f'Rerun to carry on.')

    fetched = len(missing)
    print()
    print("papers fetched this run  ", fetched)
    print("  with an abstract       ", n_with_abstract,
          f"({n_with_abstract / fetched:.1%})" if fetched else '')
    print("  unknown to the API     ", n_unknown)
    print("papers cached in total   ", len(cache))

    # What the classifier will actually have to work with, over the whole cache.
    # An abstract is worth far more than a title, so the share that has one is
    # the number that decides how good the classification can be.
    have_abstract = sum(1 for r in cache.values() if r['abstract'])
    have_tldr = sum(1 for r in cache.values() if r['tldr'])
    have_fields = sum(1 for r in cache.values() if r['fields'])
    print()
    print("over the whole cache:")
    print(f"  abstract  {have_abstract:>9}  ({have_abstract / len(cache):.1%})")
    print(f"  tldr      {have_tldr:>9}  ({have_tldr / len(cache):.1%})")
    print(f"  s2 fields {have_fields:>9}  ({have_fields / len(cache):.1%})")
    print("saved to:", cache_path)
    print("finished")


if __name__ == '__main__':
    main()

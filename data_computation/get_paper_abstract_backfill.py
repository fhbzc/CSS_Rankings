"""
Fill in the abstracts Semantic Scholar does not have, from OpenAlex.

    python get_paper_abstract_backfill.py
    python get_paper_abstract_backfill.py --mailto you@example.edu

S2's coverage of abstracts is uneven -- publisher agreements, not paper quality,
decide what it can redistribute -- so a fifth of the faculty papers come back
from get_paper_category_s1.py with nothing to judge on, including papers in
Nature and PNAS that plainly have an abstract somewhere. OpenAlex indexes from
Crossref and has a different set. This asks it for the gaps and merges what it
finds back into the same cache.

MATCHED BY DOI ONLY. OpenAlex can also be searched by title, and that would
recover more -- and would sooner or later attach the wrong paper's abstract to a
corpusid, which is a silent error that survives every check downstream. A paper
with no DOI simply stays unrecovered and is reported as such.

The DOIs come from S2, which has them for papers whose abstracts it cannot
redistribute; that asymmetry is the whole reason this works.

RESUMABLE in the same way as s1: every batch is merged into the cache before the
next is asked for, and a rerun asks only about papers that still have no
abstract. A paper OpenAlex does not know is marked as asked, so a rerun does not
ask again.

--mailto puts you in OpenAlex's polite pool, which is faster and is what their
documentation asks for. It is optional and empty by default: it sends your
address to a third party, which is your call to make and not a default to
inherit.

Input  : META_DIRE/<release_id>/cid2content.pkl   (get_paper_category_s1.py)
Output : the same file, with abstracts filled in where OpenAlex had one
"""

import argparse
import os
import pickle
import sys
import time

import requests
from tqdm import tqdm

# meta_config.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import meta_config as cfg  # noqa: E402
from meta_config import (META_DIRE, get_local_release_id,  # noqa: E402
                         require_semantic_scholar_api_key)

CONTENT_NAME = 'cid2content.pkl'

S2_BATCH_URL = 'https://api.semanticscholar.org/graph/v1/paper/batch'
S2_BATCH = 500
OPENALEX_URL = 'https://api.openalex.org/works'
# OpenAlex accepts up to 50 values in one OR filter
OPENALEX_BATCH = 50

SLEEP = 0.2
MAX_RETRIES = 4
RETRY_BACKOFF = 4.0

# marks a paper as asked-and-not-found, so a rerun skips it. Stored in the
# record rather than in a second file: the cache is already the thing that says
# what has been asked, and two files that can disagree about it is worse.
ASKED_FLAG = 'openalex_asked'


def save_cache(path, cache):
    tmp_path = path + '.tmp'
    with open(tmp_path, 'wb') as f:
        pickle.dump(cache, f, pickle.HIGHEST_PROTOCOL)
    os.replace(tmp_path, path)


def get_with_retries(session, url, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=90, **kwargs)
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_BACKOFF * attempt)
            continue
        if response.status_code == 200:
            return response.json()
        if response.status_code == 429 or response.status_code >= 500:
            if attempt == MAX_RETRIES:
                raise SystemExit(f'OpenAlex returned {response.status_code} '
                                 f'{MAX_RETRIES} times; what was recovered is '
                                 f'saved, rerun to carry on.')
            time.sleep(RETRY_BACKOFF * attempt)
            continue
        raise SystemExit(f'OpenAlex returned {response.status_code}: '
                         f'{response.text[:300]}')


def fetch_dois(session, api_key, cids):
    """{corpusid: doi} from S2, for the papers that have one."""
    dois = {}
    for start in tqdm(range(0, len(cids), S2_BATCH),
                      total=(len(cids) + S2_BATCH - 1) // S2_BATCH,
                      desc='dois'):
        batch = cids[start:start + S2_BATCH]
        response = session.post(
            S2_BATCH_URL, params={'fields': 'externalIds'},
            json={'ids': [f'CorpusId:{c}' for c in batch]},
            headers={'x-api-key': api_key}, timeout=120)
        if response.status_code != 200:
            raise SystemExit(f'the S2 API returned {response.status_code}: '
                             f'{response.text[:300]}')
        for cid, paper in zip(batch, response.json()):
            doi = ((paper or {}).get('externalIds') or {}).get('DOI')
            if doi:
                dois[cid] = doi.strip().lower()
        time.sleep(1.1)
    return dois


def reconstruct_abstract(inverted):
    """OpenAlex stores abstracts as {word: [positions]}. Put them back in order.

    Returns '' for a missing or unusable index rather than raising: a mangled
    abstract for one paper is not worth losing the run over, and an empty string
    is what the rest of the pipeline already means by "no abstract".
    """
    if not inverted:
        return ''
    positions = []
    for word, where in inverted.items():
        for position in where:
            positions.append((position, word))
    if not positions:
        return ''
    positions.sort()
    return ' '.join(word for _, word in positions)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--mailto', default='',
                    help='your email, for OpenAlex\'s polite pool. Optional; '
                         'sends your address to a third party')
    ap.add_argument('--limit', type=int, default=0, metavar='N',
                    help='only try N papers, for a trial run')
    args = ap.parse_args()

    api_key = require_semantic_scholar_api_key()
    release_id = get_local_release_id(META_DIRE, require_finished=False)
    cfg.print_config(release_id)
    meta_dire = os.path.join(META_DIRE, release_id)

    cache_path = os.path.join(meta_dire, CONTENT_NAME)
    if not os.path.isfile(cache_path):
        raise SystemExit(f'{cache_path} does not exist. '
                         f'Run get_paper_category_s1.py first.')
    with open(cache_path, 'rb') as f:
        cache = pickle.load(f)

    gaps = [cid for cid, record in cache.items()
            if not record.get('abstract') and not record.get(ASKED_FLAG)]
    already_asked = sum(1 for r in cache.values()
                        if not r.get('abstract') and r.get(ASKED_FLAG))
    print(f"{len(cache)} papers cached, "
          f"{sum(1 for r in cache.values() if not r.get('abstract'))} without an "
          f"abstract")
    print(f"  {already_asked} of those were already asked about and not found")
    print(f"  {len(gaps)} to try")
    if args.limit:
        gaps = gaps[:args.limit]
        print(f"  --limit {args.limit}")
    if not gaps:
        print("nothing to do")
        return

    session = requests.Session()

    # ==================================================
    # 1. the DOIs, from S2
    # ==================================================
    dois = fetch_dois(session, api_key, gaps)
    print(f"\n{len(dois)} of {len(gaps)} have a DOI "
          f"({len(dois) / len(gaps):.1%}); the rest cannot be matched safely "
          f"and stay unrecovered")

    # mark the DOI-less as asked: nothing more can be done for them, and a
    # rerun should not pay for the lookup again
    for cid in gaps:
        if cid not in dois:
            cache[cid][ASKED_FLAG] = True
    save_cache(cache_path, cache)

    if not dois:
        print("finished")
        return

    # ==================================================
    # 2. the abstracts, from OpenAlex
    # ==================================================
    doi2cid = {}
    for cid, doi in dois.items():
        # one DOI can be attached to two corpusids when S2 has the paper twice;
        # both should get the abstract, so the mapping is one-to-many
        doi2cid.setdefault(doi, []).append(cid)
    unique_dois = sorted(doi2cid)

    params_base = {'per-page': OPENALEX_BATCH,
                   'select': 'doi,abstract_inverted_index'}
    if args.mailto:
        params_base['mailto'] = args.mailto
    else:
        print("no --mailto: using OpenAlex's common pool, which is slower")

    n_recovered = 0
    n_empty = 0
    for start in tqdm(range(0, len(unique_dois), OPENALEX_BATCH),
                      total=(len(unique_dois) + OPENALEX_BATCH - 1) // OPENALEX_BATCH,
                      desc='openalex'):
        batch = unique_dois[start:start + OPENALEX_BATCH]
        params = dict(params_base)
        params['filter'] = 'doi:' + '|'.join(batch)
        payload = get_with_retries(session, OPENALEX_URL, params=params)

        for work in payload.get('results', []):
            doi = (work.get('doi') or '').strip().lower()
            # OpenAlex returns the DOI as a URL; the filter took it either way
            doi = doi.replace('https://doi.org/', '')
            abstract = reconstruct_abstract(work.get('abstract_inverted_index'))
            for cid in doi2cid.get(doi, []) + doi2cid.get(
                    'https://doi.org/' + doi, []):
                if abstract:
                    cache[cid]['abstract'] = abstract
                    n_recovered += 1
                else:
                    n_empty += 1

        # asked, whatever came back: a DOI OpenAlex does not know, or knows
        # without an abstract, is a dead end and rerunning will not change it
        for doi in batch:
            for cid in doi2cid[doi]:
                cache[cid][ASKED_FLAG] = True

        save_cache(cache_path, cache)
        time.sleep(SLEEP)

    still_missing = sum(1 for r in cache.values() if not r.get('abstract'))
    have_abstract = len(cache) - still_missing
    print()
    print("abstracts recovered      ", n_recovered)
    print("known to OpenAlex but with no abstract of its own:", n_empty)
    print()
    print(f"cache now: {have_abstract} of {len(cache)} papers have an abstract "
          f"({have_abstract / len(cache):.1%})")
    print(f"           {still_missing} still without one")
    print("saved to:", cache_path)
    print("finished")


if __name__ == '__main__':
    main()

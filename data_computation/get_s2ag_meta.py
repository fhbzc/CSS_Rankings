"""
Extract per-paper metadata from the Semantic Scholar Academic Graph (S2AG)
"papers" dump (gzipped JSONL shards, one paper per line). For each record (keyed
by corpusid) collect publication year, publication venue id, title, and the
list of S2 author ids. Shards are split into batches and processed in parallel,
and the per-shard dicts merged.

Venues are keyed by ID, not by name. The "venue" field of a paper record is the
venue as printed on THAT paper, so one journal arrives under many spellings --
"PNAS", "Proceedings of the National Academy of Sciences", "Proc Natl Acad Sci
U S A" are one venue written three ways, and counting or embedding them as
separate venues is simply wrong. "publicationvenueid" is S2's normalised id for
the same thing, so that is what cid2venue_id stores. The spellings are not
thrown away: venue_id2venue_name_list keeps every one seen, most frequent
first, so element [0] is a reasonable display name and the rest are the
variants. name -> id is many-to-one; id -> name is one-to-many.

Papers that carry a venue string S2 could not resolve to a venue record have no
id and therefore no entry in cid2venue_id -- to anything downstream they read as
venue-less. That is a real population change from keying on the raw string, so
the count is printed at the end rather than left to be discovered.

Only what a later step reads is collected. DOI and a global author id -> name
lookup used to be extracted here as well; nothing downstream ever loaded them,
and each was a dict with an entry per paper (per author, for the names) held in
the parent for the whole merge, so they cost real memory to produce and real
disk to keep. Anything needing one goes to the S2 API for the handful of records
it cares about.

cid2title is the expensive one and is here because get_faculty_paper_list.py
reads it: a title is ~80 bytes of unique text per paper, so on a dump of a few
hundred million papers this single dict is tens of gigabytes, in the parent
process for the whole merge and on disk afterwards. Nothing else in the pipeline
touches it. If a run is short of memory, this is the first thing to drop -- the
faculty sheet loses its title column and nothing else changes.

The citation network is built separately, by get_s2ag_citation.py.

    python get_s2ag_meta.py
    python get_s2ag_meta.py --workers 16 --batch-count 64

Input  : S2AG_DIRE/<release_id>/papers/*.gz   (S2AG papers dump; paths in utils.py)
Outputs: META_DIRE/<release_id>/cid2pub_year.pkl             corpusid -> year
         META_DIRE/<release_id>/cid2venue_id.pkl             corpusid -> venue id
         META_DIRE/<release_id>/venue_id2venue_name_list.pkl venue id -> [name]
         META_DIRE/<release_id>/cid2title.pkl                corpusid -> title
         META_DIRE/<release_id>/cid2author_list.pkl          corpusid -> [author id]

The release id is whichever release download_s2ag.py put in raw_data/ (or
RELEASE_ID from utils.py, when that is pinned), so several releases can coexist.
"""

import argparse
import json
import os
import sys
import gzip
import pickle
from collections import Counter, defaultdict

# utils.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
from utils import S2AG_DIRE, META_DIRE, get_local_release_id, process_shards  # noqa: E402

# Papers shards ship large per-shard dicts back to the parent process, so more
# workers means more of them alive at once. get_s2ag_citation.py has its own
# setting: the two dumps are worth tuning separately.
WORKERS = 4
BATCH_COUNT = 32


# single-file worker (runs in a subprocess)
def get_meta(file_path):
    corpus_id2pub_year = {}
    corpus_id2venue_id = {}
    venue_id2venue_name_count = defaultdict(Counter)
    corpus_id2title = {}
    corpus_id2author_list = {}
    stats = Counter()
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            if 'corpusid' in obj and obj['corpusid'] is not None:
                cid = obj['corpusid']
            else:
                continue
            stats['papers'] += 1

            if 'year' in obj and obj['year'] is not None:
                corpus_id2pub_year[cid] = obj['year']

            # publicationvenueid is a scalar, not a list: one paper is published
            # in one venue. It is null for a paper S2 could not match to a venue
            # record, and a missing venue is recorded by ABSENCE -- read this
            # dict with .get(cid), which returns None exactly for those.
            venue_id = obj.get('publicationvenueid')

            # S2AG writes "" (not null) for a paper with no venue at all, so the
            # empty string has to be screened out separately from None.
            venue_name = obj.get('venue')
            venue_name = venue_name.strip() if venue_name is not None else ''

            if venue_id:
                # interned: json.loads builds a fresh string per record, so
                # without this the dict holds one ~90-byte object per PAPER
                # instead of one per venue -- tens of GB on the full dump, and
                # it also lets pickle memoise the id once per shard on the way
                # back to the parent
                corpus_id2venue_id[cid] = sys.intern(venue_id)
                stats['papers_with_venue_id'] += 1
                if venue_name:
                    venue_id2venue_name_count[venue_id][venue_name] += 1
            elif venue_name:
                # a venue in print but not in S2's venue table: this paper used
                # to be counted under its raw string and now counts as venue-less
                stats['venue_name_but_no_id'] += 1

            # Not interned, unlike the venue id: a title is unique per paper,
            # so a pool would hold every one of them and buy nothing. A missing
            # or blank title is recorded by ABSENCE rather than as "" -- read
            # this dict with .get(cid, '').
            title = obj.get('title')
            if title:
                title = title.strip()
                if title:
                    corpus_id2title[cid] = title
                    stats['papers_with_title'] += 1

            # authors: [{"authorId": "2020202", "name": "A. Badawy"}, ...]
            # authorId is a string and is null for authors S2 could not resolve
            # to an author record; those carry no usable id, so they are skipped.
            # The name is not kept: the roster carries its own.
            if obj.get('authors'):
                author_id_list = [author['authorId'] for author in obj['authors']
                                  if author.get('authorId') is not None]
                if author_id_list:
                    corpus_id2author_list[cid] = author_id_list

    return (corpus_id2pub_year, corpus_id2venue_id, dict(venue_id2venue_name_count),
            corpus_id2title, corpus_id2author_list, stats)


if __name__ == "__main__":

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--workers', type=int, default=WORKERS,
                    help=f'worker processes (default {WORKERS}, 1 = run in-process)')
    ap.add_argument('--batch-count', type=int, default=BATCH_COUNT,
                    help=f'batches the shards are split into (default {BATCH_COUNT})')
    args = ap.parse_args()

    if args.workers < 1 or args.batch_count < 1:
        raise SystemExit('--workers and --batch-count must be at least 1')

    # resolved here rather than at module level: on Windows every worker
    # subprocess re-imports this file, and only the parent needs the paths
    release_id = get_local_release_id()
    print("release_id:", release_id)

    file_dire = os.path.join(S2AG_DIRE, release_id, 'papers')
    if not os.path.isdir(file_dire):
        raise SystemExit(f'{file_dire} does not exist. Run download_s2ag.py first.')

    save_dire = os.path.join(META_DIRE, release_id)
    os.makedirs(save_dire, exist_ok=True)

    corpus_id2pub_year_M = {}
    corpus_id2venue_id_M = {}
    venue_id2venue_name_count_M = defaultdict(Counter)
    corpus_id2title_M = {}
    corpus_id2author_list_M = {}
    stats_M = Counter()

    def merge_meta(result):
        (corpus_id2pub_year, corpus_id2venue_id, venue_id2venue_name_count,
         corpus_id2title, corpus_id2author_list, stats) = result
        corpus_id2pub_year_M.update(corpus_id2pub_year)
        corpus_id2title_M.update(corpus_id2title)
        corpus_id2author_list_M.update(corpus_id2author_list)
        # re-interned rather than .update()d: the worker's interning does not
        # survive the trip through pickle, so without this the master dict ends
        # up holding one string object per shard per venue again
        for cid, venue_id in corpus_id2venue_id.items():
            corpus_id2venue_id_M[cid] = sys.intern(venue_id)
        # add, not update: a venue's spellings are spread across shards and the
        # counts have to survive the merge for the "most frequent first" order
        for venue_id, name_count in venue_id2venue_name_count.items():
            venue_id2venue_name_count_M[venue_id].update(name_count)
        stats_M.update(stats)

    process_shards(get_meta, file_dire, merge_meta, "papers",
                   max_workers=args.workers, batch_count=args.batch_count)

    # most frequent spelling first, ties broken by name so reruns agree
    venue_id2venue_name_list_M = {
        venue_id: [name for name, _ in sorted(name_count.items(),
                                              key=lambda kv: (-kv[1], kv[0]))]
        for venue_id, name_count in venue_id2venue_name_count_M.items()}

    with open(os.path.join(save_dire,'cid2pub_year.pkl'),'wb') as f:
        pickle.dump(corpus_id2pub_year_M, f, pickle.HIGHEST_PROTOCOL)

    with open(os.path.join(save_dire,'cid2venue_id.pkl'),'wb') as f:
        pickle.dump(corpus_id2venue_id_M, f, pickle.HIGHEST_PROTOCOL)

    with open(os.path.join(save_dire,'venue_id2venue_name_list.pkl'),'wb') as f:
        pickle.dump(venue_id2venue_name_list_M, f, pickle.HIGHEST_PROTOCOL)

    with open(os.path.join(save_dire,'cid2title.pkl'),'wb') as f:
        pickle.dump(corpus_id2title_M, f, pickle.HIGHEST_PROTOCOL)

    with open(os.path.join(save_dire,'cid2author_list.pkl'),'wb') as f:
        pickle.dump(corpus_id2author_list_M, f, pickle.HIGHEST_PROTOCOL)

    name_count = sum(len(names) for names in venue_id2venue_name_list_M.values())
    print("papers read              ", stats_M['papers'])
    print("len cid2pub_year         ", len(corpus_id2pub_year_M))
    print("len cid2venue_id         ", len(corpus_id2venue_id_M))
    print("len cid2title            ", len(corpus_id2title_M))
    print("len cid2author_list      ", len(corpus_id2author_list_M))
    print("distinct venue ids       ", len(venue_id2venue_name_list_M))
    print("distinct venue spellings ", name_count,
          f"({name_count / max(len(venue_id2venue_name_list_M), 1):.1f} per venue)")
    # the cost of keying on the id: these papers had a venue in print but no
    # venue record to hang it on, and now read as venue-less downstream
    print("venue name but no id     ", stats_M['venue_name_but_no_id'],
          f"({stats_M['venue_name_but_no_id'] / max(stats_M['papers'], 1):.1%} of papers)")
    print("finished")

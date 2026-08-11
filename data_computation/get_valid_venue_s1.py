"""
Every venue in the release, ranked by how many papers it carries, as a CSV for
you to read.

This step only LOOKS. It produces the inventory; you go through it and decide
which venues are junk, by listing their ids in meta_config.INVALID_VENUE_IDS;
get_faculty_paper_list.py then drops every paper published in one of them.
Splitting the looking from the deciding is the point -- the deciding is manual
and the counting takes a full pass over the dump, so they should not have to
happen in the same run.

(deprecated/get_valid_venue_s2.py used to apply the same list to the WHOLE
corpus, writing out the papers that survive. Only the retired metric steps read
that file; the live pipeline screens the faculty papers directly, on the id
list, so nothing has to be recomputed over the dump when you edit it.)

    python get_valid_venue_s1.py
    python get_valid_venue_s1.py --top 20000

Venues are counted by ID, not by name. A journal that appears in the dump under
four spellings is ONE row carrying all four spellings' papers, rather than four
rows splitting them -- which is the whole reason get_s2ag_meta.py keys venues by
publicationvenueid. Every spelling seen is in the venue_name column, most
frequent first, so you can recognise the venue whichever way it was written.

Two files come out: the full list, and the top TOP_N. The top one is the one to
read -- venue counts have a very long tail of one-paper venues, and the top few
thousand cover most of the corpus.

The workflow this feeds:

  1. run this
  2. open venue_paper_count_top{TOP_N}.csv, sorted by paper_count already
  3. COPY the venue_id of every junk venue into
         meta_config.INVALID_VENUE_IDS
     one per entry, with the name in a comment. Copying is the reliable way to
     get it exact, and an id cannot be spelled wrong the way a name can.
  4. run get_faculty_paper_list.py, which applies the list

Input  : META_DIRE/<release_id>/cid2venue_id.pkl             (get_s2ag_meta.py)
         META_DIRE/<release_id>/venue_id2venue_name_list.pkl (get_s2ag_meta.py)
Output : META_DIRE/<release_id>/venue_paper_count.csv          every venue
         META_DIRE/<release_id>/venue_paper_count_top{TOP_N}.csv
"""

import argparse
import csv
import os
import pickle
import sys
from collections import Counter

# utils.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
from utils import META_DIRE, get_local_release_id  # noqa: E402

TOP_N = 10000

VENUE_NAME = 'cid2venue_id.pkl'
NAME_LIST_NAME = 'venue_id2venue_name_list.pkl'
# the name-keyed list this step used to read, kept only to help move off it
LEGACY_NAMES_PATH = os.path.join(BASE_DIR, 'manual_input', 'invalid_venues.txt')

# every spelling goes in one cell; " | " because venue names contain commas and
# semicolons often enough that either would be ambiguous to split back on
NAME_SEPARATOR = ' | '


def write_counts(path, rows, venue_id2venue_name_list, total_papers):
    cumulative = 0
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['rank', 'venue_id', 'venue_name', 'name_variants',
                         'paper_count', 'cumulative_share'])
        for rank, (venue_id, count) in enumerate(rows, start=1):
            names = venue_id2venue_name_list.get(venue_id, [])
            cumulative += count
            writer.writerow([rank, venue_id, NAME_SEPARATOR.join(names), len(names),
                             count, round(cumulative / total_papers, 6)])
    return cumulative


def report_legacy_names(venue_id2count, venue_id2venue_name_list):
    """Turn a leftover invalid_venues.txt into the venue ids it now means.

    The exclusion list used to be written in venue NAMES, in a text file.
    Those lines still describe real venues, so rather than let that curation go
    quietly stale this prints the ids they correspond to, already formatted for
    meta_config.INVALID_VENUE_IDS.
    """
    if not os.path.isfile(LEGACY_NAMES_PATH):
        return

    listed = set()
    with open(LEGACY_NAMES_PATH, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                listed.add(line)
    if not listed:
        return

    hits = {}
    for venue_id, names in venue_id2venue_name_list.items():
        matched = [n for n in names if n in listed]
        if matched and venue_id in venue_id2count:
            hits[venue_id] = matched

    print()
    print(f"note: {os.path.basename(LEGACY_NAMES_PATH)} still exists and lists "
          f"{len(listed)} venue NAMES.")
    print(f"      That file is no longer read -- venues are excluded by id now, "
          f"in meta_config.")
    print(f"      Those names resolve to {len(hits)} venue ids. Paste these into "
          f"INVALID_VENUE_IDS:")
    for venue_id, matched in sorted(hits.items(),
                                    key=lambda kv: -venue_id2count[kv[0]])[:40]:
        names = venue_id2venue_name_list.get(venue_id, [])
        print(f"        '{venue_id}',   # {names[0] if names else ''} "
              f"({venue_id2count[venue_id]} papers, matched on "
              f"{', '.join(repr(n) for n in matched[:3])})")
    if len(hits) > 40:
        print(f"        ... and {len(hits) - 40} more")

    unmatched = listed - {n for names in hits.values() for n in names}
    if unmatched:
        print(f"      {len(unmatched)} of the listed names match no venue in this "
              f"release and are simply gone: "
              f"{sorted(unmatched)[:8]}{' ...' if len(unmatched) > 8 else ''}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--top', type=int, default=TOP_N, metavar='N',
                    help=f'how many venues in the short list (default {TOP_N})')
    args = ap.parse_args()

    # resolved against META_DIRE, not raw_data: this step reads only processed
    # files, so it must still work once the raw shards have been deleted
    release_id = get_local_release_id(META_DIRE, require_finished=False)
    print("release_id", release_id)

    meta_dire = os.path.join(META_DIRE, release_id)
    venue_path = os.path.join(meta_dire, VENUE_NAME)
    name_path = os.path.join(meta_dire, NAME_LIST_NAME)
    for path in (venue_path, name_path):
        if not os.path.isfile(path):
            raise SystemExit(
                f'{path} does not exist. Run get_s2ag_meta.py first.\n'
                f'A cid2publication_venue.pkl left over from an older release is '
                f'not a substitute: venues are keyed by id now, not by name.')

    with open(venue_path, 'rb') as f:
        cid2venue_id = pickle.load(f)
    print(f"cid2venue_id load finished, papers with a venue: {len(cid2venue_id)}")

    with open(name_path, 'rb') as f:
        venue_id2venue_name_list = pickle.load(f)
    print(f"venue_id2venue_name_list load finished, venues: "
          f"{len(venue_id2venue_name_list)}")

    venue_id2count = Counter(cid2venue_id.values())
    total_papers = sum(venue_id2count.values())
    print("distinct venues:", len(venue_id2count))

    def primary_name(venue_id):
        names = venue_id2venue_name_list.get(venue_id, [])
        return names[0] if names else ''

    # most papers first; ties broken by name and then id, so the output is stable
    # across runs even for the many venues carrying one paper each
    ranked = sorted(venue_id2count.items(),
                    key=lambda kv: (-kv[1], primary_name(kv[0]), kv[0]))

    full_path = os.path.join(meta_dire, 'venue_paper_count.csv')
    write_counts(full_path, ranked, venue_id2venue_name_list, total_papers)
    print(f"wrote {full_path} ({len(ranked)} venues)")

    top_rows = ranked[:args.top]
    top_path = os.path.join(meta_dire, f'venue_paper_count_top{args.top}.csv')
    covered = write_counts(top_path, top_rows, venue_id2venue_name_list, total_papers)
    print(f"wrote {top_path} ({len(top_rows)} venues, "
          f"{covered}/{total_papers} = {covered / total_papers:.1%} of venued papers)")

    named = sum(1 for venue_id in venue_id2count
                if venue_id2venue_name_list.get(venue_id))
    print()
    print(f"{named} of {len(venue_id2count)} venues carry at least one spelling; "
          f"the rest only ever appeared on papers whose venue string was blank")

    report_legacy_names(venue_id2count, venue_id2venue_name_list)

    print()
    print("Next: put the junk venue_ids into meta_config.INVALID_VENUE_IDS, "
          "then run get_faculty_paper_list.py")
    print("finished")


if __name__ == '__main__':
    main()

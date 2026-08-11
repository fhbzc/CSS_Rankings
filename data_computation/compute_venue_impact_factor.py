"""
Each venue's impact factor, one figure per venue per year.

The Journal Impact Factor definition, computed on the S2AG citation graph:

                citations made DURING year Y to the venue's papers
                        published in Y-1 and Y-2
    IF(V, Y) = -----------------------------------------------------
                 number of the venue's papers published in Y-1, Y-2

    python compute_venue_impact_factor.py

This is NOT the JCR number. Clarivate's is computed on Web of Science coverage
with its own rules about what counts as a citable item; this is the same formula
over a different corpus, so treat it as an S2AG-native venue impact measure that
happens to be comparable in spirit, not as a JIF you can quote.

Years computed: [FOCAL_YEAR - max(CONSIDER_LENGTHS), FOCAL_YEAR - 2], from
meta_config.impact_factor_years(). Both ends differ from the focal window on
purpose. It starts one year EARLIER so a paper published in the first year of
the focal window can still be given its venue's figure from the year before --
the number an author could have seen when choosing where to submit. It stops two
years EARLIER because IF(Y) counts citations made during year Y: for Y at or
near the release date that year is not over, and a truncated count would read as
a collapse in impact rather than as missing data.

-1 means the impact factor DOES NOT EXIST for that venue and year, because the
venue published nothing in Y-1 or Y-2 and the denominator is zero. It is not a
low score, and it is not zero: 0.0 is a real figure meaning the venue published
and was never cited. Impact factors are never negative, so -1 can never collide
with a real value. Filter on it before averaging anything.

Every venue that published at least one paper anywhere in the span gets a row
for every year, -1 included. A venue absent from the whole span is left out
entirely -- rows of nothing but -1 say nothing.

The papers behind this reach TWO YEARS FURTHER BACK than the first year
computed, which is what the formula asks for. They are not restricted to the
focal window, and they are not screened by CITATION_THRESHOLD or by the invalid
venue list: an impact factor is a property of the venue over its whole output,
and screening it would make the denominator mean something else.

Input  : META_DIRE/<release_id>/cid2venue_id.pkl             (get_s2ag_meta.py)
         META_DIRE/<release_id>/cid2pub_year.pkl             (get_s2ag_meta.py)
         META_DIRE/<release_id>/venue_id2venue_name_list.pkl (get_s2ag_meta.py)
         META_DIRE/<release_id>/citing2cited_cid_list.pkl    (get_s2ag_citation.py)
Output : META_DIRE/<release_id>/venue_impact_factor_focal{FY}_len{L}.pkl
             venue id -> {year: impact factor}   (-1.0 where undefined)
         META_DIRE/<release_id>/venue_impact_factor_focal{FY}_len{L}.csv
             venue_id, venue_name, year, citable_papers, citations, impact_factor
         META_DIRE/<release_id>/venue_impact_factor_focal{FY}_len{L}_meta.json
             the settings the run used
"""

import argparse
import csv
import os
import pickle
import sys
from collections import Counter, defaultdict

from tqdm import tqdm

# meta_config.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import meta_config as cfg  # noqa: E402
from meta_config import META_DIRE, get_local_release_id  # noqa: E402

VENUE_NAME = 'cid2venue_id.pkl'
YEAR_NAME = 'cid2pub_year.pkl'
NAME_LIST_NAME = 'venue_id2venue_name_list.pkl'
CITING_NAME = 'citing2cited_cid_list.pkl'

# the value written when the denominator is zero, i.e. when the venue published
# nothing in the two years the figure is built from
UNDEFINED = -1.0

# What a reader of this file depends on. The two settings that decide which
# years are computed; nothing else fed the calculation.
DEPENDS_ON = ('release_id', 'focal_year', 'consider_lengths')

# every spelling in one cell, matching get_valid_venue_s1.py
NAME_SEPARATOR = ' | '


def impact_factor_path(meta_dire):
    """Where this release's impact factors live, under the current settings."""
    return os.path.join(meta_dire, cfg.venue_impact_factor_file_base() + '.pkl')


def load(meta_dire, release_id=None):
    """The impact factors, or a refusal.

    The entry point for anything downstream: it will not hand back a file built
    for a different focal year or set of window lengths, because that file
    covers different years. Remember that -1.0 means "no impact factor exists",
    not "an impact factor of -1".
    """
    path = impact_factor_path(meta_dire)
    if not os.path.isfile(path):
        raise SystemExit(
            f'{path} does not exist. Run compute_venue_impact_factor.py first '
            f'(a file for a different FOCAL_YEAR or CONSIDER_LENGTHS has a '
            f'different name and is not a substitute).')
    cfg.require_run_meta(path, DEPENDS_ON, release_id)
    with open(path, 'rb') as f:
        return pickle.load(f)


def write_csv(path, venue_id2year2if, venue_pubyear2count, venue_year2cites,
              venue_id2venue_name_list, years):
    """One row per (venue, year), including the years with no impact factor."""
    rows_written = 0
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['venue_id', 'venue_name', 'year', 'citable_papers',
                         'citations', 'impact_factor'])
        for venue_id in sorted(venue_id2year2if):
            names = venue_id2venue_name_list.get(venue_id, [])
            name_cell = NAME_SEPARATOR.join(names)
            for year in years:
                citable = (venue_pubyear2count.get((venue_id, year - 1), 0) +
                           venue_pubyear2count.get((venue_id, year - 2), 0))
                cites = venue_year2cites.get((venue_id, year), 0)
                value = venue_id2year2if[venue_id][year]
                writer.writerow([venue_id, name_cell, year, citable, cites,
                                 UNDEFINED if value == UNDEFINED else round(value, 6)])
                rows_written += 1
    return rows_written


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--no-csv', action='store_true',
                    help='write only the pickle. The CSV is one row per venue '
                         'per year and gets large')
    args = ap.parse_args()

    # resolved against META_DIRE, not raw_data: this step reads only processed
    # files, so it must still work once the raw shards have been deleted
    release_id = get_local_release_id(META_DIRE, require_finished=False)
    cfg.print_config(release_id)

    first_year, last_year = cfg.impact_factor_years()
    if first_year > last_year:
        raise SystemExit(
            f'the impact factor range {first_year}-{last_year} is empty: '
            f'FOCAL_YEAR {cfg.FOCAL_YEAR} with longest length '
            f'{max(cfg.CONSIDER_LENGTHS)} leaves no year at least two years '
            f'before the focal year.')
    years = list(range(first_year, last_year + 1))
    # the formula reaches two years further back than the first year computed
    source_first, source_last = first_year - 2, last_year - 1
    print(f"impact factor years {first_year}-{last_year} "
          f"(from papers published {source_first}-{source_last})")

    meta_dire = os.path.join(META_DIRE, release_id)
    paths = {
        VENUE_NAME: (os.path.join(meta_dire, VENUE_NAME), 'get_s2ag_meta.py'),
        YEAR_NAME: (os.path.join(meta_dire, YEAR_NAME), 'get_s2ag_meta.py'),
        NAME_LIST_NAME: (os.path.join(meta_dire, NAME_LIST_NAME), 'get_s2ag_meta.py'),
        CITING_NAME: (os.path.join(meta_dire, CITING_NAME), 'get_s2ag_citation.py'),
    }
    for path, script in paths.values():
        if not os.path.isfile(path):
            raise SystemExit(f'{path} does not exist. Run {script} first.')

    with open(paths[YEAR_NAME][0], 'rb') as f:
        cid2pub_year = pickle.load(f)
    print(f"cid2pub_year load finished, papers with a year: {len(cid2pub_year)}")

    with open(paths[VENUE_NAME][0], 'rb') as f:
        cid2venue_id = pickle.load(f)
    print(f"cid2venue_id load finished, papers with a venue: {len(cid2venue_id)}")

    with open(paths[NAME_LIST_NAME][0], 'rb') as f:
        venue_id2venue_name_list = pickle.load(f)

    # ==================================================
    # 1. denominators: the venue's papers, by publication year
    # ==================================================
    # Counted over the source span only. Keyed on (venue, year) rather than
    # nested, because the lookups below are all by that pair.
    venue_pubyear2count = Counter()
    for cid, venue_id in tqdm(cid2venue_id.items(), desc="citable papers"):
        year = cid2pub_year.get(cid)
        if year is not None and source_first <= year <= source_last:
            venue_pubyear2count[(venue_id, year)] += 1
    print(f"venues publishing in {source_first}-{source_last}: "
          f"{len({v for v, _ in venue_pubyear2count})}")

    # ==================================================
    # 2. numerators: citations made during each year
    # ==================================================
    # Walked from the CITING side: a citation counts by the year it was MADE, so
    # the citing paper's year decides whether the edge matters at all, and most
    # papers were published outside the range and can be dropped whole -- one
    # lookup instead of one per reference.
    with open(paths[CITING_NAME][0], 'rb') as f:
        citing2cited = pickle.load(f)
    print(f"{CITING_NAME} load finished, citing papers: {len(citing2cited)}")

    venue_year2cites = Counter()
    n_edges = 0
    with tqdm(total=len(citing2cited), desc="citations") as bar:
        while citing2cited:
            citing_cid, cited_cid_list = citing2cited.popitem()
            bar.update(1)

            citing_year = cid2pub_year.get(citing_cid)
            if citing_year is None or not (first_year <= citing_year <= last_year):
                continue

            for cited_cid in cited_cid_list:
                cited_year = cid2pub_year.get(cited_cid)
                # the two-year window the formula is built on
                if cited_year != citing_year - 1 and cited_year != citing_year - 2:
                    continue
                venue_id = cid2venue_id.get(cited_cid)
                if venue_id is None:
                    continue
                venue_year2cites[(venue_id, citing_year)] += 1
                n_edges += 1
    print(f"citations counted: {n_edges}")

    # ==================================================
    # 3. the division
    # ==================================================
    # Every venue that published anywhere in the source span gets every year,
    # UNDEFINED where its own denominator is zero. A venue absent from the whole
    # span is left out: rows of nothing but -1 say nothing.
    venues = sorted({venue_id for venue_id, _ in venue_pubyear2count})
    venue_id2year2if = {}
    defined = Counter()
    for venue_id in venues:
        by_year = {}
        for year in years:
            citable = (venue_pubyear2count.get((venue_id, year - 1), 0) +
                       venue_pubyear2count.get((venue_id, year - 2), 0))
            if citable:
                by_year[year] = venue_year2cites.get((venue_id, year), 0) / citable
                defined[year] += 1
            else:
                by_year[year] = UNDEFINED
        venue_id2year2if[venue_id] = by_year

    save_path = impact_factor_path(meta_dire)
    with open(save_path, 'wb') as f:
        pickle.dump(venue_id2year2if, f, pickle.HIGHEST_PROTOCOL)

    csv_path = None
    rows_written = 0
    if not args.no_csv:
        csv_path = os.path.join(
            meta_dire, cfg.venue_impact_factor_file_base() + '.csv')
        rows_written = write_csv(csv_path, venue_id2year2if, venue_pubyear2count,
                                 venue_year2cites, venue_id2venue_name_list, years)

    # written AFTER the data: a stamp beside a file that does not exist would be
    # worse than none, since require_run_meta() would pass on it
    meta_path = cfg.write_run_meta(
        save_path, release_id,
        impact_factor_years=[first_year, last_year],
        source_years=[source_first, source_last],
        n_venues=len(venues),
        n_citations_counted=n_edges,
        source=[VENUE_NAME, YEAR_NAME, CITING_NAME])

    print()
    print("venues with an impact factor in at least one year", len(venues))
    print("saved to  :", save_path)
    if csv_path:
        print(f"            {csv_path} ({rows_written} rows)")
    print("provenance:", meta_path)

    # Per year: how many venues the figure exists for, and where it sits. The
    # undefined count is the one to watch -- if it is most of them, the source
    # span is wrong rather than the venues idle.
    print()
    print("  year   venues with IF   undefined   median IF   mean IF")
    for year in years:
        values = sorted(v[year] for v in venue_id2year2if.values()
                        if v[year] != UNDEFINED)
        undefined = len(venues) - len(values)
        if values:
            median = values[len(values) // 2]
            mean = sum(values) / len(values)
            print(f"  {year}   {len(values):>14}   {undefined:>9}   "
                  f"{median:>9.3f}   {mean:>7.3f}")
        else:
            print(f"  {year}   {0:>14}   {undefined:>9}   "
                  f"{'--':>9}   {'--':>7}")

    # The most-cited venues of the last computed year, as a sanity check: these
    # should be recognisable, and a nonsense name at the top means the venue ids
    # or the year arithmetic are wrong.
    year = last_year
    ranked = sorted(((v[year], venue_id) for venue_id, v in venue_id2year2if.items()
                     if v[year] != UNDEFINED
                     and venue_pubyear2count.get((venue_id, year - 1), 0)
                     + venue_pubyear2count.get((venue_id, year - 2), 0) >= 100),
                    reverse=True)[:10]
    if ranked:
        print()
        print(f"highest IF in {year}, among venues with >=100 citable papers:")
        for value, venue_id in ranked:
            names = venue_id2venue_name_list.get(venue_id, [])
            print(f"  {value:>8.2f}  {names[0] if names else venue_id}")

    print("finished")


if __name__ == '__main__':
    main()

"""
Every faculty member's focal-window papers -- title, year, venue, and the
impact factor of that venue.

Four things are joined: the roster in manual_input/faculty.xlsx, the paper ->
author credits in cid2author_list.pkl, the per-paper titles and venues from
get_s2ag_meta.py, and the per-venue impact factors compute_venue_impact_factor.py
produced. The result is one row per (person, paper): what somebody published
inside the window, when, where, and how that venue stood.

The venue is given BOTH ways. venue_name is the most frequent spelling S2 saw
for it, which is what a human recognises; venue_id is the id everything upstream
keys on, which is what survives a venue being renamed and what to paste into
INVALID_VENUE_IDS. A title the dump does not carry comes out blank rather than
dropping the paper -- a title is descriptive, not something the row depends on.

n_authors counts the authors S2 RESOLVED TO AN AUTHOR ID on that paper, which is
what cid2author_list holds. Authors it could not resolve carry a null id and are
not in that list, so a paper with unresolved authors reads low here. The exact
figure would need the raw author array counted in get_s2ag_meta.py and kept as
another dict the size of the corpus; this is the count that was already on hand.
Treat it as a lower bound, and do not divide credit by it without checking how
often it is short on the papers that matter.

    python get_faculty_paper_list.py

WHICH IMPACT FACTOR a paper gets: its venue's figure from ONE reference year,
the same year for every paper, whatever year the paper itself came out in.
meta_config.impact_factor_reference_year() picks it -- the middle of the focal
window, clamped into the range impact factors exist for, which is 2020 under the
current settings.

One year for everybody, rather than matching each paper to a year near its own,
means the number says the same thing on every row: it ranks venues, and two
papers in one venue are never separated by which year they happened to appear
in. The middle of the window is the year closest to the whole window at once, so
no part of the roster is measured against a venue snapshot far from it. The year
used is kept in the sheet as if_year -- the same value on every row, so the file
still says what it was built from when it is read on its own.

A paper is DROPPED when it has no venue, when its venue is absent from the
impact factor table entirely, or when its venue has no computable figure in the
reference year. All three counts are printed separately, because they are
different problems: no venue is a metadata gap, an absent venue published
nothing in the whole span behind the table, and a -1 in the reference year is a
venue that published nothing in the two years behind THAT year. Only the last
would be fixed by choosing a different reference year, so it is the count to
watch if you move it: a year that leaves a large share of the roster without a
figure is not a reference year, however recent or however central it is.

cid2author_list is inverted for the rostered ids ONLY. The full inversion would
be one entry per author in the whole graph, tens of millions of people, and the
roster is a few thousand of them.

Keyed by NAME, because the name is what identifies a person here and the S2
author id does not: Semantic Scholar routinely splits one researcher across
several ids, and 811 of the 1091 roster rows carry more than one. Those ids are
UNIONED and the papers deduplicated by corpusid, so a paper filed under two of
someone's ids counts once. The roster's names are checked for uniqueness first
and the run stops if two rows share one -- merging two people's work is an error
that looks like a productive researcher rather than like a bug.

The impact factors come through compute_venue_impact_factor.load(), which
refuses a file built for a different FOCAL_YEAR or CONSIDER_LENGTHS. That is the
point of routing through it rather than reading the pickle: a table computed for
one configuration covers different years, and the two are impossible to tell
apart by looking.

Only papers PUBLISHED in the focal window are listed, the rule the rest of the
pipeline follows. The window is [FOCAL_YEAR - max(CONSIDER_LENGTHS) + 1,
FOCAL_YEAR] and comes from meta_config.py, so it cannot drift from what the
metric steps used. It is in the output filenames too, because it decides WHICH
papers the file holds: without that, moving FOCAL_YEAR would leave a file from
the old window sitting there looking current. The settings are stamped beside
the output as a _meta.json as well.

A paper with no publication year is NOT listed either. It cannot be placed in
the window, and guessing either way would be wrong; the count is printed.

Papers published in a venue on meta_config.INVALID_VENUE_IDS are dropped, the
list you fill in from get_valid_venue_s1.py's inventory -- preprint servers and
placeholder venues are not where somebody published, whatever impact factor the
graph can compute for them. This is where that list is applied; it is the only
other screen, and no reference or citation threshold applies here.

A paper co-authored by two rostered people appears on both of their rows, which
is what "their papers" means on each. A person with no surviving paper has NO
ROWS at all rather than one blank row, so the sheet never has to be read as
"is this an empty cell or a person with nothing". The count is printed: on a
roster that matched properly it should be modest, and a large one means the
author ids are wrong rather than the people unproductive.

The output is a workbook, not a pickle: this table is read by people, in the
same program the roster is kept in, and one row per (person, paper) is a shape
Excel handles. Long format on purpose -- a cell holding a thousand comma-
separated corpusids is unreadable, and a wide table would need as many columns
as the most prolific person has papers.

manual_input/extra_paper_credits.csv, if present, credits (person, paper) pairs
BY HAND. S2's author ids are not stable -- it merges same-named researchers into
one id and it retires ids outright, and a retired one returns nothing at all
rather than an error, taking every paper behind it out of that person's list
silently. Adopting the live id is often not the fix either, because the live one
is frequently the merged one and would import a stranger's work wholesale. That
file is the way out: name the paper and the person, and record WHY in its note
column.

Input  : manual_input/faculty.xlsx                     (your roster)
         manual_input/extra_paper_credits.csv           (optional, yours)
         META_DIRE/<release_id>/cid2author_list.pkl     (get_s2ag_meta.py)
         META_DIRE/<release_id>/cid2pub_year.pkl        (get_s2ag_meta.py)
         META_DIRE/<release_id>/cid2venue_id.pkl        (get_s2ag_meta.py)
         META_DIRE/<release_id>/cid2title.pkl           (get_s2ag_meta.py)
         META_DIRE/<release_id>/venue_id2venue_name_list.pkl (get_s2ag_meta.py)
         META_DIRE/<release_id>/venue_impact_factor_focal{FY}_len{L}.pkl
                                                (compute_venue_impact_factor.py)
Output : META_DIRE/<release_id>/faculty_paper_list_w{first}_{last}.xlsx
             one row per (person, paper)
         META_DIRE/<release_id>/faculty_paper_list_w{first}_{last}_meta.json
             the settings the run used
"""

import argparse
import csv
import os
import pickle
import sys
from collections import defaultdict

import pandas as pd
from tqdm import tqdm

# meta_config.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import meta_config as cfg  # noqa: E402
import compute_venue_impact_factor as venue_impact  # noqa: E402
from meta_config import META_DIRE, get_local_release_id  # noqa: E402
from roster import load_roster, normalise, roster_names  # noqa: E402

SOURCE_NAME = 'cid2author_list.pkl'
YEAR_NAME = 'cid2pub_year.pkl'
# (name, corpusid) pairs to credit BY HAND, for papers the author ids miss
EXTRA_CREDITS_PATH = os.path.join(BASE_DIR, 'manual_input',
                                  'extra_paper_credits.csv')
VENUE_NAME = 'cid2venue_id.pkl'
TITLE_NAME = 'cid2title.pkl'
NAME_LIST_NAME = 'venue_id2venue_name_list.pkl'

# roster columns worth repeating in the CSV beside the name, if present
CSV_EXTRA_CANDIDATES = ['affiliation', 'institution', 'university', 'country']


def read_extra_credits(names):
    """{corpusid: [name, ...]} to credit by hand, from manual_input/.

    Semantic Scholar's author ids are not stable. It merges same-named
    researchers into one id and it retires ids outright -- a roster id that
    worked when it was recorded can later return nothing at all, silently, and
    every paper behind it disappears from that person's list without an error.
    Attaching the live id is not always the fix either: the live one is often
    the merged one, and adopting it would import a stranger's papers wholesale.

    This file is the escape hatch for exactly that: name the paper and the
    person, and it is credited whatever the ids say. The note column is
    required reading rather than decoration -- a hand-placed credit with no
    reason recorded is indistinguishable from a mistake six months later.

    Unknown names are refused. A typo here would otherwise create a silent
    no-op, which is the failure this file exists to prevent.
    """
    if not os.path.isfile(EXTRA_CREDITS_PATH):
        return {}
    extra = defaultdict(list)
    unknown = []
    with open(EXTRA_CREDITS_PATH, encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            name = (row.get('name') or '').strip()
            cid = (row.get('corpusid') or '').strip()
            if not name or not cid:
                continue
            if name not in names:
                unknown.append(name)
                continue
            extra[int(cid)].append(name)
    if unknown:
        raise SystemExit(
            f'{EXTRA_CREDITS_PATH} names {len(unknown)} people who are not in '
            f'the roster: {unknown[:5]}.\n'
            f'Fix the spelling; a name that matches nobody would credit '
            f'nobody, quietly.')
    if extra:
        print(f'{sum(len(v) for v in extra.values())} hand-placed credits from '
              f'{os.path.basename(EXTRA_CREDITS_PATH)}')
    return dict(extra)


def pick_extra_columns(columns):
    """The roster columns to carry into the CSV, in the order they were asked for."""
    lookup = {normalise(c): c for c in columns}
    return [lookup[c] for c in CSV_EXTRA_CANDIDATES if c in lookup]


def venue_impact_factors(venue_id2year2if, reference_year):
    """{venue id: its impact factor in the reference year}, undefined ones dropped.

    Flattening the table to one year up front turns the per-paper question into
    a single dict lookup. It also merges "the venue is not in the table" with
    "the venue has -1 that year" into one absence, which is why the two are
    counted separately before this is consulted rather than after.
    """
    undefined = venue_impact.UNDEFINED
    return {venue_id: by_year[reference_year]
            for venue_id, by_year in venue_id2year2if.items()
            if by_year.get(reference_year, undefined) != undefined}


# Excel refuses a sheet with more rows than this, header included. The table is
# one row per (person, paper), so it is not close on a roster this size -- but a
# silent truncation would be far worse than a refusal, and openpyxl does not
# always give a clear error.
EXCEL_MAX_ROWS = 1_048_576


def build_table(df_roster, names, name_col, name2paper_list, name2author_ids,
                cid2title, venue_id2name):
    """The rows, in roster order, as a DataFrame."""
    extra_cols = pick_extra_columns(df_roster.columns)
    columns = ([name_col] + extra_cols +
               ['corpusid', 'title', 'pub_year', 'venue_name', 'venue_id',
                'impact_factor', 'if_year', 'n_authors', 'author_id'])
    rows = []
    for row_index, name in enumerate(names):
        extras = [df_roster.iloc[row_index][c] for c in extra_cols]
        for (cid, year, impact_factor, if_year, venue_id,
             n_authors) in name2paper_list.get(name, ()):
            # which of this person's ids carried the paper: the one thing you
            # need when a credit looks wrong and the roster has several
            credited = ';'.join(sorted(name2author_ids[name].get(cid, ())))
            # blank rather than absent: a paper the dump has no title for still
            # belongs on the row, and an empty cell says so plainly
            rows.append([name] + extras +
                        [cid, cid2title.get(cid, ''), year,
                         venue_id2name.get(venue_id, ''), venue_id,
                         round(impact_factor, 6), if_year, n_authors, credited])

    if len(rows) + 1 > EXCEL_MAX_ROWS:
        raise SystemExit(
            f'{len(rows)} rows will not fit in a worksheet '
            f'({EXCEL_MAX_ROWS - 1} is the limit).\n'
            f'Narrow the focal window, or ask for the CSV back -- it has no '
            f'such limit.')

    return extra_cols, pd.DataFrame(rows, columns=columns)


def write_xlsx(path, df):
    """Write the table, with the header frozen and the columns sized to read."""
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='papers')
        sheet = writer.sheets['papers']
        # the header stays put while you scroll: the table is long and the
        # columns are not self-explanatory from the values alone
        sheet.freeze_panes = 'A2'
        for i, column in enumerate(df.columns, start=1):
            # wide enough for the header and a sample of the values, capped so
            # one long affiliation does not push the numbers off the screen
            sample = df[column].head(200).astype(str).map(len).max() if len(df) else 0
            width = min(max(len(str(column)) + 2, int(sample) + 2), 48)
            sheet.column_dimensions[
                sheet.cell(row=1, column=i).column_letter].width = width


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args()

    # resolved against META_DIRE, not raw_data: this step reads only processed
    # files, so it must still work once the raw shards have been deleted
    release_id = get_local_release_id(META_DIRE, require_finished=False)
    cfg.print_config(release_id)

    # validated inside meta_config.focal_window(), which every step builds its
    # window from, so an unusable CONSIDER_LENGTHS fails the same way everywhere
    first_year, last_year = cfg.focal_window()
    first_if_year, last_if_year = cfg.impact_factor_years()
    reference_year = cfg.impact_factor_reference_year()
    print("focal window", f"{first_year}-{last_year}")
    print("impact factor years", f"{first_if_year}-{last_if_year}")
    print("impact factor REFERENCE year", reference_year,
          "-- every paper takes its venue's figure from this year")

    meta_dire = os.path.join(META_DIRE, release_id)
    source_path = os.path.join(meta_dire, SOURCE_NAME)
    year_path = os.path.join(meta_dire, YEAR_NAME)
    venue_path = os.path.join(meta_dire, VENUE_NAME)
    title_path = os.path.join(meta_dire, TITLE_NAME)
    name_list_path = os.path.join(meta_dire, NAME_LIST_NAME)
    for path in (source_path, year_path, venue_path, title_path, name_list_path):
        if not os.path.isfile(path):
            raise SystemExit(f'{path} does not exist. Run get_s2ag_meta.py first.')

    df_roster, author_id_col, roster_author_ids = load_roster()
    # raises if two rows share a name, which is the assumption everything below
    # rests on -- the check belongs before the expensive part, not after
    name_col, names = roster_names(df_roster)
    print(f"roster: {len(df_roster)} rows, {len(set(names))} distinct names, "
          f"keyed on {name_col!r}")

    extra_credits = read_extra_credits(set(names))

    # one person, several ids: which ids belong to whom
    author_id2name = {}
    for name, author_ids in zip(names, roster_author_ids):
        for author_id in author_ids:
            author_id2name[author_id] = name
    print(f"{len(author_id2name)} distinct author ids over those rows")

    # through load(), not pickle.load(): it checks the table was built for the
    # focal year and lengths this run is working in, and refuses it otherwise
    venue_id2year2if = venue_impact.load(meta_dire, release_id)
    print(f"loaded impact factors for {len(venue_id2year2if)} venues from "
          f"{os.path.basename(venue_impact.impact_factor_path(meta_dire))}")
    venue2impact_factor = venue_impact_factors(venue_id2year2if, reference_year)
    print(f"{len(venue2impact_factor)} of them have a figure in {reference_year}")

    invalid_venues = cfg.invalid_venue_ids()
    print(f"{len(invalid_venues)} venues on INVALID_VENUE_IDS will be screened out")

    with open(year_path, 'rb') as f:
        cid2pub_year = pickle.load(f)
    print(f"loaded {len(cid2pub_year)} papers with a year from {YEAR_NAME}")

    with open(venue_path, 'rb') as f:
        cid2venue_id = pickle.load(f)
    print(f"loaded {len(cid2venue_id)} papers with a venue from {VENUE_NAME}")

    with open(name_list_path, 'rb') as f:
        venue_id2venue_name_list = pickle.load(f)
    # only the most frequent spelling is kept: the sheet needs one readable
    # name per row, and the rest of the variants live in the file above
    venue_id2name = {vid: names[0]
                     for vid, names in venue_id2venue_name_list.items() if names}
    print(f"loaded names for {len(venue_id2name)} venues from {NAME_LIST_NAME}")

    with open(title_path, 'rb') as f:
        cid2title = pickle.load(f)
    print(f"loaded {len(cid2title)} titles from {TITLE_NAME}")

    with open(source_path, 'rb') as f:
        cid2author_list = pickle.load(f)
    print(f"loaded {len(cid2author_list)} papers from {SOURCE_NAME}")

    # popitem() as we go: cid2author_list is one list per paper in the whole
    # dump and the result is a few thousand lists, so draining the source hands
    # nearly all of that memory back while the inversion is still running
    name2papers = defaultdict(dict)          # name -> {cid: (year, if, if_year)}
    name2author_ids = defaultdict(dict)      # name -> {cid: {author id, ...}}
    n_outside = 0
    n_no_year = 0
    n_no_venue = 0
    n_invalid_venue = 0
    n_hand_placed = 0
    n_venue_absent = 0
    n_no_impact_factor = 0
    with tqdm(total=len(cid2author_list), desc="invert") as bar:
        while cid2author_list:
            cid, author_list = cid2author_list.popitem()
            bar.update(1)

            # the window screen goes FIRST: it drops most of the dump, and the
            # id lookup below is the expensive part
            year = cid2pub_year.get(cid)
            if year is None:
                # counted only when a rostered author wrote it, so the numbers
                # mean "papers we lost", not "papers in the dump like this"
                if any(author_id in author_id2name for author_id in author_list):
                    n_no_year += 1
                continue
            if not (first_year <= year <= last_year):
                n_outside += 1
                continue

            credited = [a for a in author_list if a in author_id2name]
            hand_placed = extra_credits.get(cid, ())
            if not credited and not hand_placed:
                continue

            venue_id = cid2venue_id.get(cid)
            if venue_id is None:
                n_no_venue += 1
                continue

            # before the impact factor is consulted: a paper in a venue you have
            # declared invalid is dropped for THAT reason, and mixing it into
            # the impact factor counts would hide how many there were
            if venue_id in invalid_venues:
                n_invalid_venue += 1
                continue

            impact_factor = venue2impact_factor.get(venue_id)
            if impact_factor is None:
                # told apart here, because the flattened dict cannot: a venue
                # missing from the whole table is a different problem from one
                # that simply has no figure in this particular year
                if venue_id in venue_id2year2if:
                    n_no_impact_factor += 1
                else:
                    n_venue_absent += 1
                continue

            for author_id in credited:
                name = author_id2name[author_id]
                # keyed by cid, so two of one person's ids on one paper collapse
                # to a single credit rather than counting the paper twice
                name2papers[name][cid] = (year, impact_factor, reference_year,
                                          venue_id, len(author_list))
                name2author_ids[name].setdefault(cid, set()).add(author_id)

            # ... and the ones placed by hand, which by definition have no
            # author id of this person's on the paper
            for name in hand_placed:
                if cid in name2papers[name]:
                    continue          # an id already caught it; nothing to add
                n_hand_placed += 1
                name2papers[name][cid] = (year, impact_factor, reference_year,
                                          venue_id, len(author_list))
                name2author_ids[name].setdefault(cid, set()).add('(by hand)')

    if extra_credits:
        wanted = sum(len(v) for v in extra_credits.values())
        print(f"hand-placed credits applied: {n_hand_placed} of {wanted}")
        if n_hand_placed < wanted:
            print(f"  the rest were already caught by an author id, or their "
                  f"paper failed one of the screens below")

    print(f"focal window: {n_outside} papers dropped as published outside it")
    dropped = (n_no_venue + n_invalid_venue + n_venue_absent
               + n_no_impact_factor)
    print()
    print(f"DROPPED: {dropped} papers by rostered authors")
    print(f"  no venue at all          {n_no_venue}")
    print(f"  venue is INVALID         {n_invalid_venue}   "
          f"(on meta_config.INVALID_VENUE_IDS)")
    print(f"  venue not in the table   {n_venue_absent}   "
          f"(published nothing in the whole span behind it)")
    print(f"  venue is -1 in {reference_year}      {n_no_impact_factor}   "
          f"(the only one a different reference year could rescue)")
    if n_no_year:
        print(f"  no publication year      {n_no_year}   "
              f"(could not be placed in the window either)")

    # chronological, ties by corpusid: a paper list reads by date, and the tie
    # break keeps a rerun on the same release byte-identical
    name2paper_list = {
        name: sorted(((cid, year, impact_factor, if_year, venue_id, n_authors)
                      for cid, (year, impact_factor, if_year, venue_id, n_authors)
                      in papers.items()),
                     key=lambda p: (p[1], p[0]))
        for name, papers in name2papers.items()}

    extra_cols, df_out = build_table(df_roster, names, name_col,
                                     name2paper_list, name2author_ids,
                                     cid2title, venue_id2name)
    rows_written = len(df_out)

    save_path = os.path.join(meta_dire, cfg.faculty_paper_list_file_base() + '.xlsx')
    write_xlsx(save_path, df_out)

    # written AFTER the data: a stamp beside a file that does not exist would be
    # worse than none, since require_run_meta() would pass on it
    meta_path = cfg.write_run_meta(
        save_path, release_id,
        n_roster_rows=int(len(df_roster)),
        n_author_ids=len(author_id2name),
        n_faculty_with_paper=len(name2paper_list),
        n_paper_credits=rows_written,
        impact_factor_reference_year=reference_year,
        n_dropped_no_venue=n_no_venue,
        n_dropped_invalid_venue=n_invalid_venue,
        n_dropped_venue_absent=n_venue_absent,
        n_dropped_no_impact_factor=n_no_impact_factor,
        n_hand_placed_credits=n_hand_placed,
        n_credits_without_title=sum(
            1 for papers in name2paper_list.values()
            for cid, _, _, _, _, _ in papers if cid not in cid2title),
        source=[SOURCE_NAME, YEAR_NAME, VENUE_NAME,
                os.path.basename(venue_impact.impact_factor_path(meta_dire))])

    counts = sorted(len(name2paper_list.get(n, ())) for n in names)
    found = [c for c in counts if c]
    matched_ids = {a for name in name2author_ids
                   for ids in name2author_ids[name].values() for a in ids}
    print()
    print("faculty with >=1 paper   ", f"{len(found)} of {len(names)}")
    print("faculty with no paper    ", len(names) - len(found))
    print("author ids that matched  ", f"{len(matched_ids)} of {len(author_id2name)}")
    print("paper credits in total   ", rows_written, "(a co-authored paper counts "
          "once per person)")
    if found:
        print("papers per faculty member",
              f"min {found[0]}, median {found[len(found) // 2]}, max {found[-1]}")

    # The impact factors are the point of the join, so say something about them
    # rather than leaving the CSV to be opened before anyone notices a problem.
    values = sorted(f for papers in name2paper_list.values()
                    for _, _, f, _, _, _ in papers)
    if values:
        print()
        print(f"impact factor over every credit (all from {reference_year}):")
        print(f"  mean   {sum(values) / len(values):.3f}")
        print(f"  median {values[len(values) // 2]:.3f}")
        print(f"  at 0   {sum(1 for v in values if v == 0.0)} "
              f"(venue published, was never cited that year)")

    author_counts = sorted(n for papers in name2paper_list.values()
                           for _, _, _, _, _, n in papers)
    if author_counts:
        solo = sum(1 for n in author_counts if n == 1)
        print()
        print("authors per paper (S2-resolved ids only, so a LOWER BOUND):")
        print(f"  median {author_counts[len(author_counts) // 2]}, "
              f"max {author_counts[-1]}, "
              f"{solo} single-author ({solo / len(author_counts):.1%})")

    print()
    print(f"saved to  : {save_path} ({rows_written} rows, "
          f"roster columns carried: {extra_cols or 'none found'})")
    print("provenance:", meta_path)
    print("finished")


if __name__ == '__main__':
    main()

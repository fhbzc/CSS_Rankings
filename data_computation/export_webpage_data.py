"""
Turn the faculty paper list into the files the webpage reads.

The site ranks people by what they published in a DOMAIN over a YEAR WINDOW, so
the numbers change with both. That rules out the one-row-per-person shape the
export used to have: two metrics times 21 areas times four windows is 168
columns, and adding a window would mean re-exporting.

So the per-person file is emitted LONG and the site sums it over whatever
window the reader picks. A new window then costs nothing.

The grouping is (person, year, AREA COMBINATION) -- not (person, year, area).
That is what makes a paper countable ONCE when the reader selects two domains
it belongs to. Pre-aggregating per area throws the combination away, and no
amount of arithmetic downstream recovers it: a paper in both A and B is
indistinguishable from two papers, one in each. Grouping by the combination
keeps every paper attributable to exactly one row, while still collapsing the
thousands of papers that share a person, a year and a set of domains.

    python data_computation/export_webpage_data.py
    python data_computation/export_webpage_data.py --check    # validate only

TWO METRICS, both summed, never averaged:

    paper_count   how many of that person's papers fall in the selected
                  domains and years, each counted once
    weighted_if   sum over those same papers of impact_factor / n_authors

weighted_if credits a paper to its authors in equal shares: a five-author paper
with an impact factor of 10 contributes 2, a solo paper with 2 contributes 2.
Summing rather than averaging is deliberate and it decides how the site reads --
a large department outranks a small one, and prolific outranks selective. That
is the intended ranking, not an oversight.

A paper counted in two areas is ONE paper. Selecting both domains counts it
once, and its weight once -- the site is showing what somebody published, and a
paper does not become two by spanning two fields. Selecting either domain alone
also counts it once. This is why the rows carry an area COMBINATION rather than
a single area.

WHAT IS NOT EMITTED. A paper in no area at all has no row, and neither does a
(person, year, combination) with nothing in it -- so the site never has to
distinguish "zero" from "missing". There is only absence, and absence means the
person does not appear under that domain. An institution whose every member is
absent under the reader's filters likewise does not appear.

n_authors is the count of authors Semantic Scholar RESOLVED to an author id, so
it runs low where authors went unresolved, and weighted_if therefore runs high.
The bias is larger for papers with big author lists. It is recorded in
metrics.json rather than shown.

TWO SETS OF FILES COME OUT, written straight to where they are read from, and
from one read of the inputs so the site and the repository can never describe
different rosters:

  ../webpage/processed_data/  the aggregates the SITE serves -- long by (person,
                              year, area combination), plus metrics.json
  ../upload_version/data/     the roster and the paper list THEMSELVES, as the
                              public CSVs the GitHub repository publishes and
                              takes corrections against

Both directories are overwritten in place. Run --check first if you want to see
what a run would produce without touching either; the previous contents of the
site directory are copied to a .bak_<date>/ beside them, so the last served
version is always one move away.

The public files are CSV because GitHub cannot preview or diff a workbook: a
pull request against one shows "binary file changed" and nothing else, so a
correction cannot be reviewed. They are rebuilt whole on every run rather than
patched, which is what stops them drifting from the workbooks one edit at a
time -- the workbooks are where the data is edited, these are derived.

Input  : META_DIRE/<release_id>/faculty_paper_list_w{first}_{last}.xlsx
             (get_faculty_paper_list.py, then candidate_pipeline/apply_paper_areas.py)
         manual_input/faculty.xlsx   (for homepage, and the area taxonomy)
Output : ../webpage/processed_data/{metrics.json,faculty.csv,faculty_papers.csv}
         ../upload_version/data/{faculty.csv,faculty_paper_list_w{first}_{last}.csv}
         --check writes neither

See webpage/DATA_FORMAT.md for the schema these files follow.
"""

import argparse
import datetime
import io
import json
import os
import re
import shutil
import sys
from collections import defaultdict

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import meta_config as cfg  # noqa: E402
from meta_config import META_DIRE, get_local_release_id  # noqa: E402
from roster import load_roster, normalise, roster_names  # noqa: E402

WEB_DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), 'webpage', 'processed_data')

FILES = ('metrics.json', 'faculty.csv', 'faculty_papers.csv')

# The public data files, for the GitHub repository -- the roster and the paper
# list themselves, not the aggregates above. They are written from the SAME run
# so the published data and the served site can never describe different rosters.
PUBLIC_DIR = os.path.join(os.path.dirname(BASE_DIR), 'upload_version', 'data')

# Columns the public files carry. Selected, not copied wholesale: the workbooks
# hold working columns a reader has no use for -- author_id (Semantic Scholar's,
# unstable) and the roster's own area columns, and on the paper side the internal
# join keys corpusid / venue_id / if_year / author_id. affiliation and country
# are dropped from the paper list too: they describe the PERSON, so faculty.csv
# is the one place they are read from and there is no second copy to fall stale.
PUBLIC_ROSTER_COLUMNS = ['name', 'affiliation', 'country', 'homepage']
PUBLIC_PAPER_LEADING = ['name', 'title', 'pub_year', 'venue_name',
                        'impact_factor', 'n_authors']
PUBLIC_PAPER_TRAILING = ['n_areas', 'area_evidence']

_WHITESPACE = re.compile(r'\s+')

# The two metrics the site offers. Both are sums over the papers in the reader's
# (domain, window) selection, and both aggregate to an institution by summing
# again -- there is no mean, at any level.
METRICS = [
    {
        'key': 'paper_count',
        'label': 'Paper count',
        'description': 'Papers published in this area during the window.',
        'higherIsBetter': True,
        'decimals': 0,
    },
    {
        'key': 'weighted_if',
        # The name says "sum" because that is the whole point: it is summed over
        # papers and summed again over an institution's members, never averaged.
        'label': 'Weighted IF sum',
        'description': ('Sum over those papers of the venue impact factor '
                        'divided by the number of authors, so a paper is '
                        'credited to its authors in equal shares.'),
        'higherIsBetter': True,
        'decimals': 2,
    },
]

HOMEPAGE_CANDIDATES = ['homepage', 'website', 'url', 'page']


def clean_homepage(value):
    """Turn a roster homepage cell into either a usable URL or an empty string.

    `str(value or '')` is not enough: an empty cell arrives as float NaN, and
    NaN is truthy, so the fallback never fires and the literal text 'nan' ends
    up in the CSV. The site treats any non-empty value as a link, so those rows
    rendered as live hyperlinks pointing at 'nan'.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ''
    text = str(value).strip()
    if not text or text.lower() in ('nan', 'none', 'null', 'n/a', '-'):
        return ''
    if text.lower().startswith(('http://', 'https://')):
        return text
    # a bare domain is a typo, not a scheme choice; keep the intent
    return f'https://{text}' if '.' in text and ' ' not in text else ''


def backup_served_files():
    """Copy what the site is currently serving into .bak_<date>/ beside it.

    This step used to write to manual_output/ and leave the copy into the site
    directory to you, so a re-export could never overwrite what was being served
    by surprise. Writing straight to the site directory is one less step and one
    less way for the two to disagree, but it does overwrite -- so the version
    coming out is kept, and going back is a move rather than a rebuild.

    Dated, not numbered: what you want to answer later is "what was the site
    serving before today's run", and a date answers it. Several runs on one day
    overwrite the same directory, which is the right behaviour -- the useful
    baseline is the last one from BEFORE you started changing things.
    """
    existing = [f for f in FILES if os.path.isfile(os.path.join(WEB_DATA_DIR, f))]
    if not existing:
        return None
    stamp = datetime.date.today().strftime('%Y%m%d')
    backup_dire = os.path.join(WEB_DATA_DIR, f'.bak_{stamp}')
    os.makedirs(backup_dire, exist_ok=True)
    for name in existing:
        shutil.copy2(os.path.join(WEB_DATA_DIR, name),
                     os.path.join(backup_dire, name))
    print(f'\nprevious site data -> {backup_dire}  ({", ".join(existing)})')
    return backup_dire


def _tidy_whitespace(df):
    """Collapse every run of whitespace in text cells to one space.

    A newline inside a quoted CSV field is legal, but it puts one record on
    several physical lines, and GitHub's line-level diff is the whole reason
    these files are CSV. The cases in practice are line-wrap artifacts in
    Semantic Scholar titles and non-breaking spaces, so nothing is lost.
    """
    out = df.copy()
    changed = 0
    for column in out.columns:
        if out[column].dtype != object:
            continue
        before = out[column]
        after = before.map(
            lambda v: _WHITESPACE.sub(' ', v).strip() if isinstance(v, str) else v)
        changed += int((before.astype(str) != after.astype(str)).sum())
        out[column] = after
    return out, changed


def write_public_csv(df, path):
    """Write one public CSV, then read it back and refuse a run that lost a cell.

    utf-8-sig, because thousands of cells hold non-ASCII characters and Excel
    opens a plain UTF-8 CSV in the system codepage, mangling every one of them
    for a contributor who double-clicks it. GitHub renders and diffs a BOM'd
    file the same as a plain one. '\\n' endings so the diff is identical on
    every platform.
    """
    df.to_csv(path, index=False, encoding='utf-8-sig', lineterminator='\n')

    back = pd.read_csv(path, encoding='utf-8-sig')
    name = os.path.basename(path)
    assert list(back.columns) == list(df.columns), f'{name}: columns differ after round trip'
    assert len(back) == len(df), f'{name}: {len(back)} rows back, {len(df)} written'
    a = df.astype(str).replace({'nan': ''})
    b = back.astype(str).replace({'nan': ''})
    differ = (a.values != b.values)
    if differ.any():
        rows, cols = differ.nonzero()
        for r, c in list(zip(rows, cols))[:5]:
            print(f'  MISMATCH row {r} {df.columns[c]!r}: '
                  f'{a.iat[r, c]!r} != {b.iat[r, c]!r}')
        raise SystemExit(f'{name}: {int(differ.sum())} cells differ after round trip')

    lines = io.open(path, encoding='utf-8-sig').read().count('\n')
    assert lines == len(df) + 1, \
        f'{name}: {lines} lines for {len(df)} rows + header -- a field spans lines'
    print(f'  {name}: {len(df)} rows x {len(df.columns)} cols, '
          f'{os.path.getsize(path) / 1e6:.1f} MB, round trip OK')


def export_public_data(df_roster, sheet, areas, paper_csv_name):
    """Write upload_version/data/ -- the roster and paper list the repo publishes.

    Rebuilt whole from the workbooks every time rather than patched, so the
    published CSVs cannot drift from them one edit at a time. The workbooks are
    where the data is edited; these are derived.
    """
    os.makedirs(PUBLIC_DIR, exist_ok=True)
    print(f'\npublic data -> {PUBLIC_DIR}')

    missing = [c for c in PUBLIC_ROSTER_COLUMNS if c not in df_roster.columns]
    if missing:
        raise SystemExit(f'the roster is missing {missing}, which the public file needs')
    roster_out, n = _tidy_whitespace(df_roster[PUBLIC_ROSTER_COLUMNS])
    print(f'  roster: {n} cells tidied')
    write_public_csv(roster_out, os.path.join(PUBLIC_DIR, 'faculty.csv'))

    wanted = PUBLIC_PAPER_LEADING + list(areas) + PUBLIC_PAPER_TRAILING
    missing = [c for c in wanted if c not in sheet.columns]
    if missing:
        raise SystemExit(f'the paper list is missing {missing}, which the public file needs')
    papers_out, n = _tidy_whitespace(sheet[wanted])
    print(f'  papers: {n} cells tidied')
    write_public_csv(papers_out, os.path.join(PUBLIC_DIR, paper_csv_name))


def find_column(columns, candidates):
    lookup = {normalise(c): c for c in columns}
    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--check', action='store_true',
                    help='validate the inputs and report what would be written, '
                         'without writing anything')
    args = ap.parse_args()

    release_id = get_local_release_id(META_DIRE, require_finished=False)
    cfg.print_config(release_id)
    first_year, last_year = cfg.focal_window()

    sheet_path = os.path.join(META_DIRE, release_id,
                              cfg.faculty_paper_list_file_base() + '.xlsx')
    if not os.path.isfile(sheet_path):
        raise SystemExit(
            f'{sheet_path} does not exist.\n'
            f'Run get_faculty_paper_list.py, then '
            f'candidate_pipeline/apply_paper_areas.py to add the area columns.')

    sheet = pd.read_excel(sheet_path)
    df_roster, _, _ = load_roster(verbose=False)
    name_col, _ = roster_names(df_roster)
    areas = [c for c in df_roster.columns if c in sheet.columns and '/' in str(c)]
    if not areas:
        raise SystemExit(
            f'{os.path.basename(sheet_path)} carries no area columns.\n'
            f'Run candidate_pipeline/apply_paper_areas.py first -- without it '
            f'there is nothing to break the numbers down by.')
    print(f'{len(sheet)} (person, paper) rows, {len(areas)} areas')

    for column in ('impact_factor', 'n_authors', 'pub_year'):
        if column not in sheet.columns:
            raise SystemExit(f'{os.path.basename(sheet_path)} has no {column} '
                             f'column; it cannot be the sheet this expects.')

    # ==================================================
    # 1. the long table
    # ==================================================
    # Keyed on (name, year, area combination) -- see the note in the docstring
    # on why the combination and not the individual area.
    counts = defaultdict(int)
    weighted = defaultdict(float)
    n_bad_authors = 0
    AREA_SEP = '|'

    # Columns pulled out as arrays rather than read off itertuples: the area
    # names carry spaces and slashes, which itertuples renames to _1, _2, ...,
    # and addressing them by position through that renaming is a bug waiting
    # for someone to reorder a column.
    names = sheet['name'].astype(str).str.strip().to_numpy()
    years = sheet['pub_year'].astype(int).to_numpy()
    impact = sheet['impact_factor'].astype(float).to_numpy()
    authors = sheet['n_authors'].astype(int).to_numpy()
    flags = sheet[areas].to_numpy()

    for i in range(len(sheet)):
        row_areas = [area for j, area in enumerate(areas) if flags[i, j] == 1]
        if not row_areas:
            continue
        n_authors = int(authors[i])
        if n_authors < 1:
            # cannot divide by it; the paper still counts, its weight does not
            n_bad_authors += 1
            share = 0.0
        else:
            share = float(impact[i]) / n_authors
        key = (names[i], int(years[i]), AREA_SEP.join(row_areas))
        counts[key] += 1
        weighted[key] += share

    rows = [{'name': name, 'year': year, 'areas': areakey,
             'paper_count': n, 'weighted_if': round(weighted[(name, year, areakey)], 4)}
            for (name, year, areakey), n in sorted(counts.items())]
    long_table = pd.DataFrame(rows, columns=['name', 'year', 'areas',
                                             'paper_count', 'weighted_if'])
    if n_bad_authors:
        print(f'warning: {n_bad_authors} rows had no usable author count; their '
              f'papers count but carry no weight')

    # ==================================================
    # 2. the people
    # ==================================================
    # Only people who appear somewhere in the long table: a person with no
    # papers in any area cannot be ranked under any filter, and shipping them
    # would put a permanently empty row in the site.
    present = set(long_table['name'])
    homepage_col = find_column(df_roster.columns, HOMEPAGE_CANDIDATES)
    people = []
    for _, person in df_roster.iterrows():
        name = str(person[name_col]).strip()
        if name not in present:
            continue
        people.append({
            'name': name,
            'affiliation': str(person.get('affiliation', '') or '').strip(),
            'country': str(person.get('country', '') or '').strip(),
            'homepage': (clean_homepage(person[homepage_col])
                         if homepage_col else ''),
        })
    faculty = pd.DataFrame(people, columns=['name', 'affiliation', 'country',
                                            'homepage'])

    dropped = len(df_roster) - len(faculty)
    print(f'\n{len(faculty)} people ship; {dropped} have no papers in any area '
          f'and are left out entirely')
    print(f'{len(long_table)} (person, year, area-combination) rows')
    print(f'  papers      {int(long_table.paper_count.sum())}'
          f'   (each counted once, whatever it spans)')
    print(f'  weighted_if {long_table.weighted_if.sum():.0f}')

    # Per area, a paper spanning two is counted under each, so these do NOT add
    # up to the total above -- that total counts every paper once.
    print('\nby area (a paper spanning two appears under both):')
    per_area = defaultdict(lambda: [set(), 0, 0.0])
    for r in long_table.itertuples(index=False):
        for area in r.areas.split(AREA_SEP):
            slot = per_area[area]
            slot[0].add(r.name)
            slot[1] += r.paper_count
            slot[2] += r.weighted_if
    for area, (people, papers, wif) in sorted(per_area.items(),
                                              key=lambda kv: -kv[1][1]):
        print(f'  {len(people):>5} people {papers:>6} papers '
              f'{wif:>9.0f} wIF  {area}')

    spanning = int(long_table[long_table.areas.str.contains(r'\|')]
                   .paper_count.sum())
    print(f'\n{spanning} papers span more than one area '
          f'({spanning / int(long_table.paper_count.sum()):.1%}); selecting both '
          f'their domains counts each of them ONCE')

    # ==================================================
    # 3. write
    # ==================================================
    metrics = {
        'focalYear': cfg.FOCAL_YEAR,
        'years': [first_year, last_year],
        'windows': sorted(cfg.CONSIDER_LENGTHS),
        # weighted_if leads: paper count alone rewards volume, and the whole
        # point of dividing by author count was to stop a long author list
        # counting as a whole paper for everyone on it
        'defaultMetric': 'weighted_if',
        'defaultWindow': max(cfg.CONSIDER_LENGTHS),
        'metrics': METRICS,
        'areas': areas,
        'dataset': {'source': 'Semantic Scholar', 'releaseId': release_id},
        'impactFactor': {
            'year': cfg.impact_factor_reference_year(),
            'note': ('Every paper carries its venue\'s impact factor from one '
                     'reference year, so the figure ranks venues rather than '
                     'tracking a venue over time.'),
        },
        'caveats': [
            'n_authors counts the authors Semantic Scholar resolved to an '
            'author id, so it runs low where authors went unresolved and '
            'weighted_if therefore runs high, most for papers with long author '
            'lists.',
            'Institution figures are sums over their members. There is no mean '
            'at any level, so a large department outranks a small one.',
        ],
    }

    if args.check:
        print('\n--check: nothing written')
        return

    os.makedirs(WEB_DATA_DIR, exist_ok=True)
    backup_served_files()

    faculty.to_csv(os.path.join(WEB_DATA_DIR, 'faculty.csv'), index=False)
    long_table.to_csv(os.path.join(WEB_DATA_DIR, 'faculty_papers.csv'),
                      index=False)
    with open(os.path.join(WEB_DATA_DIR, 'metrics.json'), 'w',
              encoding='utf-8') as f:
        json.dump(metrics, f, indent=1, ensure_ascii=False)
        f.write('\n')

    # the old wide export's files would otherwise sit beside the new ones and be
    # read as current
    for stale in ('institutions.csv', 'faculty_yearly.csv',
                  'faculty_area_year.csv'):
        path = os.path.join(WEB_DATA_DIR, stale)
        if os.path.isfile(path):
            os.remove(path)
            print(f'removed stale {stale} from the previous export')

    print(f'\nsite data -> {WEB_DATA_DIR}')
    print(f'  wrote {", ".join(FILES)}')

    export_public_data(df_roster, sheet, areas,
                       cfg.faculty_paper_list_file_base() + '.csv')
    print('finished')


if __name__ == '__main__':
    main()

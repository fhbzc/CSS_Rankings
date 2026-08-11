"""
Read manual_input/faculty.xlsx and turn its author id column into S2 author ids.

This lives on its own because more than one step needs it and the two must not
disagree about who a person is. The parsing is fiddlier than it looks -- Excel
turns digit strings into floats, and one researcher routinely holds several S2
author ids -- so a second copy that drifted by one rule would have the metrics
and the paper lists quietly describing different people.

Kept out of utils.py deliberately: utils is imported by get_s2ag_meta.py and
get_s2ag_citation.py, whose worker subprocesses re-import it on every spawn, and
pandas is not a cost those should pay to read a dump.
"""

import os
import re

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROSTER_PATH = os.path.join(BASE_DIR, 'manual_input', 'faculty.xlsx')

# column names in the roster that may hold the S2 author id, tried in order.
# Headers are matched case-insensitively with spaces/hyphens folded to '_'.
AUTHOR_ID_CANDIDATES = ['author_id', 'authorid', 's2_author_id', 'author', 'id']

# and the ones that may hold the person's name
NAME_CANDIDATES = ['name', 'faculty_name', 'full_name', 'person']


def normalise(name):
    return str(name).strip().lower().replace(' ', '_').replace('-', '_')


def _find_column(columns, candidates, what):
    lookup = {normalise(c): c for c in columns}
    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]
    raise SystemExit(
        f'no {what} column in {ROSTER_PATH}.\n'
        f'Columns found : {list(columns)}\n'
        f'Names accepted: {candidates} (case-insensitive, spaces and '
        f'hyphens treated as underscores).')


def find_author_id_column(columns):
    """The roster column holding the author id, or raise with what was found."""
    return _find_column(columns, AUTHOR_ID_CANDIDATES, 'author id')


def find_name_column(columns):
    """The roster column holding the person's name, or raise with what was found."""
    return _find_column(columns, NAME_CANDIDATES, 'name')


def roster_names(df_roster):
    """One stripped name per roster ROW, checked for the uniqueness it is used on.

    Names are what identifies a person in the outputs, so two rows sharing one
    would silently merge two people's work into a single record -- an error that
    looks like a productive researcher rather than like a bug. The roster is
    yours to fix, so the check names the offenders instead of guessing.
    """
    name_col = find_name_column(df_roster.columns)
    names = [str(v).strip() for v in df_roster[name_col]]

    blank = [i for i, n in enumerate(names) if not n or n.lower() == 'nan']
    if blank:
        raise SystemExit(
            f'{len(blank)} roster rows have a blank {name_col!r} (rows '
            f'{blank[:10]}{" ..." if len(blank) > 10 else ""}).\n'
            f'Names identify people in the output, so every row needs one.')

    seen = {}
    duplicates = {}
    for i, name in enumerate(names):
        if name in seen:
            duplicates.setdefault(name, [seen[name]]).append(i)
        else:
            seen[name] = i
    if duplicates:
        shown = '\n'.join(f'  {name!r}: rows {rows}'
                          for name, rows in sorted(duplicates.items())[:10])
        raise SystemExit(
            f'{len(duplicates)} names appear on more than one roster row:\n{shown}\n'
            f'Names identify people in the output, so two rows sharing one would '
            f'merge their work. Disambiguate them in {ROSTER_PATH}.')

    return name_col, names


# S2 author ids are strings in the dumps; Excel readily turns them into numbers,
# so both sides are normalised to a plain digit string before matching.
def clean_author_id(value):
    text = str(value).strip()
    if text.endswith('.0'):        # 2020202 read back as 2020202.0
        text = text[:-2]
    return text or None


def parse_author_ids(value):
    """The ids of ONE person, as a list.

    Semantic Scholar routinely splits a researcher across several author ids, and
    a cell may therefore carry more than one, separated by ';' (or ',' / '|').
    Every id listed is treated as the same person and their papers should be
    UNIONED by corpusid -- taking just one id would silently drop the rest of
    their work and report the remainder as if it were their whole output.
    """
    if pd.isna(value):
        return []
    parts = re.split(r'[;,|\s]+', str(value).strip())
    ids = [clean_author_id(p) for p in parts]
    # dedupe, keep order
    return list(dict.fromkeys(i for i in ids if i))


def load_roster(verbose=True):
    """(df_roster, author_id_col, roster_author_ids).

    roster_author_ids is one list of ids per roster ROW, positionally aligned
    with df_roster, so a row with no usable id is an empty list rather than a
    missing entry -- callers stay row-indexed either way.
    """
    if not os.path.isfile(ROSTER_PATH):
        raise SystemExit(
            f'{ROSTER_PATH} does not exist.\n'
            'Put your faculty list there: one row per person, with an author id '
            'column (plus affiliation / name / anything else you want carried '
            'through to the output).')

    df_roster = pd.read_excel(ROSTER_PATH)
    if verbose:
        print(f"loaded {os.path.basename(ROSTER_PATH)}: {len(df_roster)} rows, "
              f"columns {list(df_roster.columns)}")
    assert len(df_roster) > 0, f'{ROSTER_PATH} has no rows'

    author_id_col = find_author_id_column(df_roster.columns)
    if verbose:
        print("author id column:", repr(author_id_col))

    # Re-read that one column as text. The ids are stored as strings in the
    # workbook, but read_excel re-infers a column of digit strings as float64,
    # which turns 47786922 into 47786922.0 and would drop digits from anything
    # past 2^53. clean_author_id() still strips a stray ".0" from a hand-edited
    # file.
    df_roster[author_id_col] = pd.read_excel(
        ROSTER_PATH, usecols=[author_id_col], dtype=str)[author_id_col]

    roster_author_ids = [parse_author_ids(v) for v in df_roster[author_id_col]]

    if verbose:
        n_blank = sum(1 for ids in roster_author_ids if not ids)
        n_multi = sum(1 for ids in roster_author_ids if len(ids) > 1)
        if n_blank:
            print(f"warning: {n_blank} roster rows have a blank author id")
        if n_multi:
            print(f"{n_multi} roster rows carry several author ids; their papers "
                  f"are unioned by corpusid, so a paper under two of a person's "
                  f"ids counts once")

    return df_roster, author_id_col, roster_author_ids

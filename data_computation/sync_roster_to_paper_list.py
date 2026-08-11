"""Copy the roster's per-person columns into the faculty paper list, keyed by name.

get_faculty_paper_list.py already stamps affiliation and country onto every row it
writes, so a freshly built sheet agrees with the roster by construction. This is
for afterwards: the roster is edited by hand between builds -- an affiliation
corrected, two spellings of one university unified -- and rebuilding the sheet to
carry the change would mean re-reading the whole S2AG dump for a column that has
nothing to do with the dump.

    python sync_roster_to_paper_list.py            # show what would change
    python sync_roster_to_paper_list.py --write    # write it

KEYED BY NAME, the same key get_faculty_paper_list.py groups by, and the roster is
checked for duplicate names first -- two rows sharing one would push one person's
affiliation onto the other's papers. A name in the sheet that the roster no longer
has is REPORTED AND LEFT ALONE rather than blanked: it means the person was
removed from the roster after the sheet was built, and blanking their affiliation
would hide that instead of showing it. Rebuild the sheet to drop them properly.

Only the columns named in SYNC_COLUMNS are touched, and only where the sheet and
the roster actually disagree; every other cell is written back byte for byte.

Input  : manual_input/faculty.xlsx                     (the roster, authoritative)
         META_DIRE/<release_id>/faculty_paper_list_w{first}_{last}.xlsx
Output : the same paper list, with those columns refreshed (--write only)
"""

import argparse
import os
import shutil
import sys

import pandas as pd

# meta_config.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import meta_config as cfg  # noqa: E402
from meta_config import META_DIRE, get_local_release_id  # noqa: E402
from roster import load_roster, roster_names  # noqa: E402

# The per-person columns the roster owns. The paper list carries them for
# convenience -- they describe the PERSON, not the paper, so the roster is where
# they are edited and this is the direction the copy runs.
SYNC_COLUMNS = ['affiliation', 'country']


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--write', action='store_true',
                    help='write the changes (default is a dry run)')
    ap.add_argument('--columns', nargs='+', default=SYNC_COLUMNS,
                    help=f'roster columns to copy (default: {SYNC_COLUMNS})')
    args = ap.parse_args()

    release_id = get_local_release_id(META_DIRE, require_finished=False)
    cfg.print_config(release_id)
    meta_dire = os.path.join(META_DIRE, release_id)
    sheet_path = os.path.join(meta_dire, cfg.faculty_paper_list_file_base() + '.xlsx')
    if not os.path.isfile(sheet_path):
        raise SystemExit(f'{sheet_path} does not exist. Run get_faculty_paper_list.py first.')

    df_roster, _, _ = load_roster()
    name_col, names = roster_names(df_roster)      # raises on a duplicate name

    columns = [c for c in args.columns if c in df_roster.columns]
    missing = [c for c in args.columns if c not in df_roster.columns]
    if missing:
        print(f'not in the roster, skipped: {missing}')
    if not columns:
        raise SystemExit('none of the requested columns are in the roster')

    lookup = {col: dict(zip(names, df_roster[col])) for col in columns}

    print(f'reading {os.path.basename(sheet_path)} ...')
    sheet = pd.read_excel(sheet_path)
    print(f'  {len(sheet)} rows, {sheet["name"].nunique()} distinct names')

    unknown = sorted(set(sheet['name']) - set(names))
    if unknown:
        n_rows = int(sheet['name'].isin(unknown).sum())
        print(f'\n{len(unknown)} names in the sheet are NOT in the roster '
              f'({n_rows} rows) -- left untouched:')
        for n in unknown[:10]:
            print(f'    {n}')
        if len(unknown) > 10:
            print(f'    ... and {len(unknown) - 10} more')

    total = 0
    for col in columns:
        if col not in sheet.columns:
            print(f'\n{col}: not a column of the sheet, added')
            sheet[col] = pd.NA
        current = sheet[col].astype(str)
        wanted = sheet['name'].map(lookup[col])
        # only where the roster knows the person AND the two disagree
        known = sheet['name'].isin(names)
        changed = known & (current != wanted.astype(str))
        n = int(changed.sum())
        total += n
        print(f'\n{col}: {n} rows differ from the roster')
        if n:
            pairs = (pd.DataFrame({'from': current[changed], 'to': wanted[changed].astype(str)})
                     .value_counts().head(20))
            for (frm, to), c in pairs.items():
                print(f'    {c:5d}  {frm!r}')
                print(f'           -> {to!r}')
        sheet.loc[changed, col] = wanted[changed]

    print(f'\n{total} cells to update in total')
    if not total:
        print('nothing to do')
        return
    if not args.write:
        print('dry run -- pass --write to apply')
        return

    backup = sheet_path + '.bak_sync'
    shutil.copy2(sheet_path, backup)
    sheet.to_excel(sheet_path, index=False)
    print(f'backup  {backup}')
    print(f'written {sheet_path}')


if __name__ == '__main__':
    main()

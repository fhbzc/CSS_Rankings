"""The one place every step of the pipeline reads its settings from.

Edit this file to describe a run: which S2AG release it is built on, which year it
is anchored to, and how far back the windows reach. Nothing else in
data_computation/ carries a knob of its own -- the scripts import from here, so a
setting cannot be changed in one step and left stale in the next.

Two things follow from FOCAL_YEAR and CONSIDER_LENGTHS and are worth stating
plainly, because they decide how much work the pipeline does:

  The focal window is [FOCAL_YEAR - max(CONSIDER_LENGTHS) + 1, FOCAL_YEAR].
  Only papers PUBLISHED inside it ever reach the output: the site sums over
  windows that all end at FOCAL_YEAR, and the longest one starts at that lower
  bound. So get_faculty_paper_list.py lists those papers only.

  This is not a filter on the evidence. The impact factors those papers carry are
  computed over the venue's own history, which reaches back before the window and
  is not restricted by it. The window says which papers are worth listing, not
  which links count.

Change FOCAL_YEAR or the largest entry of CONSIDER_LENGTHS and the computed
tables no longer cover what the run needs; the steps that read them check the
window recorded alongside the data and refuse rather than silently under-report.

Every setting in this file is LIVE. The retired per-paper novelty metrics (E/F/G,
disruption, atypicality, the citation and metric quantiles) had eight settings and
six helpers of their own; they are commented out in one block at the END of this
file, so nothing here describes a knob that no longer does anything. Uncomment
that block to bring those steps back.
"""

import json
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ==================================================
# where things live
# ==================================================
S2AG_DIRE = os.path.join(BASE_DIR, 'raw_data')   # raw downloaded shards (S2AG / S2ORC)
META_DIRE = os.path.join(BASE_DIR, 'data')       # processed files

# Semantic Scholar API key, required by download_s2ag.py to download the bulk
# datasets. Get one at https://www.semanticscholar.org/product/api
#
# It is read from the environment, never written here: this file is public, and a
# key committed to a repository is a leaked key. Set it before running a step
# that talks to the API --
#
#     PowerShell   $env:SEMANTIC_SCHOLAR_API_KEY = 'your-key'
#     cmd.exe      set SEMANTIC_SCHOLAR_API_KEY=your-key
#     bash/zsh     export SEMANTIC_SCHOLAR_API_KEY='your-key'
#
# To keep it across sessions on Windows, set it once for your user account:
#     [Environment]::SetEnvironmentVariable('SEMANTIC_SCHOLAR_API_KEY', 'your-key', 'User')
#
# Steps that do not touch the API run fine without it; the ones that do raise a
# clear error (see require_semantic_scholar_api_key below) rather than sending an
# unauthenticated request and failing later with an opaque 403.
SEMANTIC_SCHOLAR_API_KEY = os.environ.get('SEMANTIC_SCHOLAR_API_KEY', '')


def require_semantic_scholar_api_key():
    """Return the key, or explain how to set it. Call this before an API request."""
    key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY', '')
    if not key:
        raise RuntimeError(
            'SEMANTIC_SCHOLAR_API_KEY is not set.\n'
            "  PowerShell:  $env:SEMANTIC_SCHOLAR_API_KEY = 'your-key'\n"
            '  cmd.exe:     set SEMANTIC_SCHOLAR_API_KEY=your-key\n'
            "  bash/zsh:    export SEMANTIC_SCHOLAR_API_KEY='your-key'\n"
            'Get a key at https://www.semanticscholar.org/product/api')
    return key

# ==================================================
# dataset version
# ==================================================
# The S2AG release everything is built from, e.g. '2026-07-28'. Raw shards live in
# raw_data/<RELEASE_ID>/ and processed files in data/<RELEASE_ID>/, so several
# releases coexist. Leave empty to use the newest release present on disk; pin it
# to hold the whole pipeline on one release after a newer one is published.
#
# It is also the date the site dates the ranking by: the release id IS a date, and
# it is the one that says how current the numbers are. There is deliberately no
# second "when did we download it" knob -- the download date says nothing about
# the numbers, and two dates that drift apart is a bug waiting to happen.
RELEASE_ID = '2026-07-28'

# ==================================================
# the run
# ==================================================
# The year every window ends at, inclusive.
FOCAL_YEAR = 2025

# Window lengths, in years. Length L covers [FOCAL_YEAR - L + 1, FOCAL_YEAR], both
# ends inclusive, and becomes its own set of _last{L}y columns.
CONSIDER_LENGTHS = [1, 2, 5, 10]

# ==================================================
# venues that do not count as publication
# ==================================================
# The venues to treat as INVALID: preprint servers, placeholder strings, whatever
# else you decide is not a publication venue. get_faculty_paper_list.py drops
# every paper published in one of them.
#
# Semantic Scholar publicationvenueids, one per line, with the venue's name in a
# comment -- a bare list of UUIDs is unreadable six months later, and the comment
# is the only thing that makes this list maintainable.
#
# THE UNIT IS THE ID, NOT THE NAME. One id covers every spelling S2 filed under
# it, so a single entry takes "arXiv", "arxiv", "ARXIV" and "arXiv.org" together
# when S2 considers them one venue. Excluding by name needed an entry per
# spelling, and a spelling nobody thought of let its papers through in silence.
#
# The list comes out of your data, not out of guesswork:
#
#   1. run get_valid_venue_s1.py
#   2. open data/<release_id>/venue_paper_count_top10000.csv -- already sorted by
#      paper_count, with every spelling of each venue in venue_name
#   3. COPY the venue_id of each junk venue in here
#   4. run get_faculty_paper_list.py, which applies the list
#
# The list is recorded in that step's _meta.json, so a sheet built before you
# edited it can be told apart afterwards. Empty means nothing is excluded, so
# only papers with no venue at all drop out.
INVALID_VENUE_IDS = [
    '1901e811-ee72-4b20-8f7e-de08cd395a10', # arXiv.org
    '75d7a8c1-d871-42db-a8e4-7cf5146fdb62', # Social Science Research Network
    '027ffd21-ebb0-4af8-baf5-911124292fd0', # bioRxiv
    'd5e5b5e7-54b1-4f53-82fc-4853f3e71c58', # medRxiv
]

# ==================================================
# derived
# ==================================================
_RELEASE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# download_s2ag.py writes this into raw_data/<release>/<dataset>/ once every shard
# of that dataset is present, so an interrupted download is never mistaken for a
# complete one.
FINISHED_MARKER = 'finished.pkl'


def focal_window():
    """(first_year, last_year) of the focal window, both ends inclusive."""
    assert CONSIDER_LENGTHS, 'CONSIDER_LENGTHS is empty'
    assert all(isinstance(L, int) and L >= 1 for L in CONSIDER_LENGTHS), \
        f'CONSIDER_LENGTHS must be positive integers: {CONSIDER_LENGTHS}'
    return FOCAL_YEAR - max(CONSIDER_LENGTHS) + 1, FOCAL_YEAR


def in_focal_window(year):
    """True when a paper published in `year` can reach the output at all."""
    if year is None:
        return False
    first, last = focal_window()
    return first <= year <= last


def describe_window():
    first, last = focal_window()
    return (f'focal window {first}-{last} '
            f'(FOCAL_YEAR {FOCAL_YEAR}, longest length {max(CONSIDER_LENGTHS)})')


def window_tag():
    """The focal window, as it appears in filenames.

    Every per-paper metric file carries it, because the focal window decides WHICH
    papers are in the file. Without it in the name, moving FOCAL_YEAR would make
    the resumable steps reuse shards computed for the old window and report them
    as though they covered the new one.
    """
    first, last = focal_window()
    return f'w{first}_{last}'


def impact_factor_years():
    """(first_year, last_year) of the years an impact factor is computed for.

    [FOCAL_YEAR - max(CONSIDER_LENGTHS), FOCAL_YEAR - 2], both ends inclusive.

    It is NOT the focal window, and deliberately so at both ends. It starts one
    year earlier, so that a paper published in the first year of the focal
    window can still be given the impact factor its venue had the year before --
    the number an author could have seen. It stops two years earlier, because
    IF(Y) counts citations made DURING year Y: for Y at or near the release date
    that year is not over, and the figure would read as a collapse in impact
    when it is only a truncated count.
    """
    assert CONSIDER_LENGTHS, 'CONSIDER_LENGTHS is empty'
    return FOCAL_YEAR - max(CONSIDER_LENGTHS), FOCAL_YEAR - 2


def invalid_venue_ids():
    """INVALID_VENUE_IDS as a set, tidied.

    Whitespace stripped and blanks dropped, so a list edited by hand does not
    fail on a stray space or a trailing comma's worth of empty string. Every
    step that screens on the list comes through here, so none of them can
    disagree about what is on it.
    """
    return {str(v).strip() for v in INVALID_VENUE_IDS if str(v).strip()}


def impact_factor_reference_year():
    """The ONE year whose impact factors the faculty paper list uses.

    The middle of the focal window, clamped into the range impact factors are
    actually computed for. With FOCAL_YEAR 2025 and a longest length of 10 the
    window is 2016-2025 and this is 2020.

    The middle rather than the newest, because a single reference year is a
    compromise for every paper that is not from that year, and the middle is the
    year closest to the whole window at once -- it minimises the worst case. The
    newest computable year would be five years off the oldest papers.

    The clamp matters when the window is short: a window of [2025, 2025] has its
    middle at 2025, which no impact factor exists for (they stop at
    FOCAL_YEAR - 2), so the reference falls back to the newest year that does.
    """
    first, last = focal_window()
    middle = first + (last - first) // 2
    if_first, if_last = impact_factor_years()
    return min(max(middle, if_first), if_last)


def venue_impact_factor_file_base():
    """Base name shared by compute_venue_impact_factor.py's outputs.

    Spelled as focal{FOCAL_YEAR}_len{longest length} rather than as the window
    tag the other steps use, because the years here are not the focal window:
    they run to FOCAL_YEAR - 2, and the papers behind them reach two years
    further back still, so w{first}_{last} would describe the span wrongly. The
    two settings that decide the range are named instead.
    """
    return f'venue_impact_factor_focal{FOCAL_YEAR}_len{max(CONSIDER_LENGTHS)}'


def faculty_paper_list_file_base():
    """Base name shared by get_faculty_paper_list.py's outputs.

    The focal window, and only that. It decides which papers are listed, and it
    also decides the impact factor years the step joins against -- window_tag()
    is built from FOCAL_YEAR and the longest length, which is exactly what
    impact_factor_years() derives from, so naming both would say one thing
    twice. INVALID_VENUE_IDS changes the contents too but is not in the name --
    it is a list, not a value a filename can carry; the _meta.json records it.

    The roster changes the contents too, of course, but manual_input/faculty.xlsx
    has no version to put in a name -- the _meta.json records its row and id
    counts instead, which is what tells you afterwards whether the roster moved
    under a file.
    """
    return f'faculty_paper_list_{window_tag()}'


def _release_is_complete(release_dire):
    """True when every dataset subdirectory of a release carries FINISHED_MARKER."""
    subdires = [d for d in sorted(os.listdir(release_dire))
                if os.path.isdir(os.path.join(release_dire, d))]
    if not subdires:
        return False
    return all(os.path.isfile(os.path.join(release_dire, d, FINISHED_MARKER))
               for d in subdires)


def dataset_version(release_id=None):
    """{'release_id'} -- the provenance stamped onto every output.

    One key, deliberately: the release id is the only date that describes the
    numbers, and it needs no resolving or caching because it is already the name
    of the directory they were read from. Kept as a dict so the shape is defined
    here rather than rebuilt by each caller.
    """
    release_id = release_id or get_local_release_id(META_DIRE, require_finished=False)
    return {'release_id': release_id}


def get_local_release_id(base_dire=None, require_finished=True):
    """RELEASE_ID if set, otherwise the newest usable release already on disk.

    Never touches the network: the processing steps must run on the release that
    was actually downloaded, and once a newer one is published the API's "latest"
    no longer matches what is in raw_data/.

    base_dire defaults to S2AG_DIRE (raw_data). Steps that read only processed
    files should pass META_DIRE, so they keep working after the raw shards are
    deleted; those pass require_finished=False, since META_DIRE's release
    directories hold processed files rather than dataset directories.
    """
    base = S2AG_DIRE if base_dire is None else base_dire
    if RELEASE_ID:
        return RELEASE_ID

    releases = []
    if os.path.isdir(base):
        releases = sorted(d for d in os.listdir(base)
                          if _RELEASE_RE.match(d) and os.path.isdir(os.path.join(base, d)))

    if not releases:
        raise SystemExit(
            f'no release directory (e.g. 2026-07-28) found under {base}.\n'
            'Run download_s2ag.py first, or pin RELEASE_ID in meta_config.py.')

    if not require_finished:
        return releases[-1]

    complete = [r for r in releases if _release_is_complete(os.path.join(base, r))]
    if complete:
        return complete[-1]

    raise SystemExit(
        f'no complete release found under {base}.\n'
        f'Releases present but unfinished: {", ".join(releases)}\n'
        f'A release counts as complete once every dataset subdirectory holds a '
        f'{FINISHED_MARKER}, which download_s2ag.py writes when that dataset has '
        f'all of its shards. Re-run download_s2ag.py to resume, or pin RELEASE_ID '
        f'in meta_config.py to force one.')


# ==================================================
# provenance: stamp the settings beside an output, and refuse a stale one
# ==================================================
# A file on disk says nothing about the settings it was built under, and the
# settings live in this file where they are edited between runs. Reading an
# output computed for FOCAL_YEAR 2024 as though it covered 2025 is the failure
# these two functions exist to prevent: the producer stamps, the reader checks,
# and a mismatch stops the run instead of quietly under-reporting.
#
# The filename carries the settings that make two files DIFFERENT, so they can
# coexist. The stamp carries everything the run was configured with, including
# settings the step did not read -- it costs nothing and is the record you want
# when a number looks wrong six months later.

def run_meta(release_id=None, **extra):
    """The settings block a step stamps beside its output."""
    first, last = focal_window()
    meta = {
        'release_id': dataset_version(release_id)['release_id'],
        'focal_year': FOCAL_YEAR,
        'consider_lengths': list(CONSIDER_LENGTHS),
        'focal_window': [first, last],
        # the list itself, not its length: this is the record that says which
        # venues a file was built without, and a count would not
        'invalid_venue_ids': sorted(invalid_venue_ids()),
    }
    meta.update(extra)
    return meta


def run_meta_path(data_path):
    """<data_path without its extension>_meta.json."""
    return os.path.splitext(data_path)[0] + '_meta.json'


def write_run_meta(data_path, release_id=None, **extra):
    """Stamp the current settings beside data_path. Returns the stamp's path."""
    path = run_meta_path(data_path)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(run_meta(release_id, **extra), f, indent=1)
        f.write('\n')
    return path


def require_run_meta(data_path, keys, release_id=None):
    """Load the stamp beside data_path, refusing it unless `keys` still match.

    `keys` is what the READER depends on, and naming it at the call site is the
    point: a step that only cares about the focal window should not be stopped
    because some setting it never read moved, and a step that does care must say
    so. Returns the stamp on success.

    A missing stamp is a hard error rather than a shrug. It means the file
    predates this mechanism or was written by something else, and either way
    there is no way to tell what it covers -- which is exactly the situation the
    check exists to refuse.
    """
    path = run_meta_path(data_path)
    if not os.path.isfile(path):
        raise SystemExit(
            f'{path} does not exist, so there is no record of the settings\n'
            f'{data_path} was built with. Delete the file and rebuild it, rather '
            f'than reading it and hoping.')

    with open(path, encoding='utf-8') as f:
        stamp = json.load(f)

    current = run_meta(release_id)
    differences = []
    for key in keys:
        if key not in stamp:
            differences.append(f'  {key}: not recorded, now {current[key]!r}')
        elif stamp[key] != current[key]:
            differences.append(f'  {key}: built with {stamp[key]!r}, '
                               f'meta_config.py now says {current[key]!r}')

    if differences:
        raise SystemExit(
            f'{data_path} was built with settings that no longer match '
            f'meta_config.py:\n' + '\n'.join(differences) +
            f'\nRebuild it, or put the settings back. Reading it as it stands '
            f'would report one configuration\'s numbers as another\'s.')

    return stamp


def print_config(release_id=None):
    """One banner every step prints, so a log says what it was run with."""
    version = dataset_version(release_id)
    first, last = focal_window()
    print(f"[meta_config] release {version['release_id']}")
    print(f"[meta_config] focal_year {FOCAL_YEAR} | consider_lengths {CONSIDER_LENGTHS} "
          f"| focal window {first}-{last} inclusive")


# =============================================================================
# RETIRED: the per-paper novelty metrics
# =============================================================================
# Everything below belonged to the metric steps now under deprecated/ -- E/F/G
# generalization, CD disruption, reference atypicality, and the citation and
# metric quantiles built on them. None of it is read by the live pipeline, which
# ranks on venue impact factors alone, so it is commented out rather than left
# to look like a knob that still does something.
#
# TO BRING THE METRICS BACK: uncomment this whole block, and re-add the two
# lines marked below to run_meta() and print_config(). Nothing else in this file
# has to change -- the live settings above are the ones those steps share.
#
# ---- settings ---------------------------------------------------------------
#
# Citation window for E/F/G and disruption: only citations arriving within
# YEAR_OFFSET years of the focal paper's publication count. -1 disables it, so
# every citation counts. Read by deprecated/get_efg.py, get_disruption.py, their
# merge steps, compute_metric_quantile.py and the atypicality scripts, which all
# have to agree: it is part of the filenames they hand each other.
# YEAR_OFFSET = -1
#
# Upper end of the citation window, inclusive. None = the newest publication year
# in the data, so nothing is dropped for being recent. An int caps it instead,
# which keeps results stable across releases.
# MAX_YEAR = None
#
# Count only papers deprecated/get_valid_venue_s2.py kept -- those published in a
# venue that is not on the exclusion list. This changes what a quantile means (a
# paper is ranked against its valid-venue peers rather than every same-year
# paper), so it is tagged into the filenames and the two variants never overwrite
# each other.
# ONLY_VALID_VENUE = False
#
# Screens which papers are quantiled at all.
# REFERENCE_THRESHOLD = 1
# CITATION_THRESHOLD = 1
#
# What REFERENCE_THRESHOLD counts: every reference (False), or only those
# published in a valid venue (True). Changes which papers are screened in,
# nothing else.
# REFERENCE_THRESHOLD_VALID_VENUE_ONLY = True
#
# Drop papers published in or after this year before quantiling. None = keep all.
# QUANTILE_MAX_YEAR = None
#
# How many shards the per-paper metrics are computed in. Each shard is written
# and flagged separately, so a run is resumable and can be split across machines.
# The compute step and its merge step must agree on this number -- they address
# the same files by it -- which is why it lived here rather than in either.
# DIVISION_COUNT = 20
#
# ---- filename helpers -------------------------------------------------------
#
# def venue_suffix():
#     return '_validvenue' if ONLY_VALID_VENUE else ''
#
#
# def rt_tag():
#     return 'rtvv' if REFERENCE_THRESHOLD_VALID_VENUE_ONLY else 'rt'
#
#
# def efg_file_base():
#     """Base name shared by deprecated/get_efg.py and get_efg_s2_merge.py."""
#     return f'efg_y{YEAR_OFFSET}_{window_tag()}'
#
#
# def disruption_file_base():
#     """Base name shared by deprecated/get_disruption.py and arrange_disruption.py."""
#     return f'disp_y{YEAR_OFFSET}_{window_tag()}'
#
#
# def quantile_file_name():
#     """The per-paper quantile table deprecated/compute_metric_quantile.py writes.
#
#     Carries the focal window like every other per-paper file, so a table
#     computed for one window is never read as though it covered another.
#     Matching by name is deliberate: the alternative -- inferring coverage from
#     which years have rows -- misfires whenever CITATION_THRESHOLD legitimately
#     leaves a recent year empty.
#     """
#     return (f'metric_quantile_y{YEAR_OFFSET}_{window_tag()}{venue_suffix()}_'
#             f'{rt_tag()}_{REFERENCE_THRESHOLD}_'
#             f'ct_{CITATION_THRESHOLD}_'
#             f'ybefore_{"all" if QUANTILE_MAX_YEAR is None else QUANTILE_MAX_YEAR}.csv')
#
#
# def citation_quantile_file_base():
#     """Base name shared by deprecated/compute_citation_quantile.py's outputs.
#
#     The focal window decides which papers are ranked and CITATION_THRESHOLD
#     decides which of those qualify, so both are in the name: two files built
#     under different values describe different populations and must not be
#     mistaken for one another.
#
#     YEAR_OFFSET, ONLY_VALID_VENUE and REFERENCE_THRESHOLD are NOT here, because
#     that step does not read them -- the counts behind it are unwindowed and
#     unfiltered, and it has no reference counts to screen on. Naming a setting
#     the step never read would force a recompute every time it moved. The
#     _meta.json beside the file records all of them anyway, and
#     require_run_meta() is what a reader checks it with.
#     """
#     return f'citation_quantile_{window_tag()}_ct_{CITATION_THRESHOLD}'
#
# ---- the two lines to put back ----------------------------------------------
#
# in run_meta(), inside the dict:
#         'year_offset': YEAR_OFFSET,
#         'max_year': MAX_YEAR,
#         'only_valid_venue': ONLY_VALID_VENUE,
#         'reference_threshold': REFERENCE_THRESHOLD,
#         'citation_threshold': CITATION_THRESHOLD,
#         'reference_threshold_valid_venue_only': REFERENCE_THRESHOLD_VALID_VENUE_ONLY,
#         'quantile_max_year': QUANTILE_MAX_YEAR,
#
# in print_config(), as the last line:
#     print(f"[meta_config] year_offset {YEAR_OFFSET} | only_valid_venue {ONLY_VALID_VENUE} "
#           f"| reference_threshold {REFERENCE_THRESHOLD} "
#           f"| citation_threshold {CITATION_THRESHOLD}")

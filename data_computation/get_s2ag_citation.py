"""
Build the S2AG citation network from the Semantic Scholar Academic Graph (S2AG)
"citations" dump (gzipped JSONL shards, one citation edge per line).

Edges are NOT filtered against the papers dump: every edge with both endpoints
present is kept, including ones whose endpoints have no metadata.

Two steps, because the network is the biggest thing this pipeline holds:

  build   read the shards in parallel and merge them into citing -> [cited]
          only, then save it. Only one direction is built here, so the second
          one is not competing for memory during the expensive merge.
  invert  load citing -> [cited] back in and turn it round into
          cited -> [citing], draining the source as it goes so the two are
          never both fully resident.

    python get_s2ag_citation.py                      # both steps
    python get_s2ag_citation.py --workers 16
    python get_s2ag_citation.py --step invert        # redo the reverse only

build is resumable at batch granularity: a batch that finishes is written to
META_DIRE/<release_id>/citation_build/ before the next one starts, and a rerun
skips the batches already there. Interrupting it therefore costs the batch in
flight, not the run. The checkpoints are deleted once the forward file is
written; --no-checkpoint turns the whole thing off for a machine short on disk,
since while build runs the edges are held both in memory and on disk.

Input  : S2AG_DIRE/<release_id>/citations/*.gz   (S2AG citations dump; paths in utils.py)
Outputs: META_DIRE/<release_id>/citing2cited_cid_list.pkl   citing corpusid -> [cited corpusids]
         META_DIRE/<release_id>/cited2citing_cid_list.pkl   cited corpusid -> [citing corpusids]
Scratch: META_DIRE/<release_id>/citation_build/batch_{i}_max_{n}.pkl (removed on success)

The release id is whichever release download_s2ag.py put in raw_data/ (or
RELEASE_ID from utils.py, when that is pinned), so several releases can coexist.
"""

import argparse
import json
import os
import shutil
import sys
import gzip
import pickle
from collections import defaultdict

from tqdm import tqdm

# utils.py is in this same directory; make it importable regardless of CWD.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
from utils import S2AG_DIRE, META_DIRE, get_local_release_id, process_shards  # noqa: E402

# Citations shards are numerous and mostly JSON parsing. get_s2ag_meta.py has
# its own setting: the two dumps are worth tuning separately.
WORKERS = 4
BATCH_COUNT = 32

FORWARD_NAME = 'citing2cited_cid_list.pkl'
REVERSE_NAME = 'cited2citing_cid_list.pkl'

# Per-batch checkpoints live here while build runs, and the directory is removed
# once the forward file is safely written. They are scratch, not output: holding
# the edges twice is only worth it while there is a run to salvage.
CHECKPOINT_DIRNAME = 'citation_build'


def _batch_path(ckpt_dire, batch, batch_count):
    # batch_count is in the name because it decides which shards land in which
    # batch: checkpoints from a run with a different --batch-count describe
    # different work and must not be mistaken for this run's
    return os.path.join(ckpt_dire, f'batch_{batch}_max_{batch_count}.pkl')


def _write_batch(ckpt_dire, batch, batch_count, files, citing2cited):
    """Check one finished batch out to disk, atomically.

    Written to a temp name and renamed, because a kill during a multi-GB dump
    would otherwise leave a truncated pickle that the next run sees as done and
    reloads as a batch that is quietly missing most of its edges.
    """
    path = _batch_path(ckpt_dire, batch, batch_count)
    tmp_path = path + '.tmp'
    with open(tmp_path, 'wb') as f:
        pickle.dump({'files': list(files), 'citing2cited': dict(citing2cited)},
                    f, pickle.HIGHEST_PROTOCOL)
    os.replace(tmp_path, path)


def _warn_about_foreign_checkpoints(ckpt_dire, batch_count):
    """Say so when checkpoints from a different --batch-count are lying around.

    They are never loaded -- the count is in the file name -- but silently
    recomputing everything while the disk fills with two sets of edges is the
    kind of thing worth a line of output.
    """
    suffix = f'_max_{batch_count}.pkl'
    foreign = [f for f in sorted(os.listdir(ckpt_dire))
               if f.startswith('batch_') and f.endswith('.pkl')
               and not f.endswith(suffix)]
    if foreign:
        print(f'note: {len(foreign)} checkpoint(s) in {ckpt_dire} were written with a '
              f'different --batch-count and will be ignored, not resumed '
              f'(e.g. {foreign[0]}). Delete them to reclaim the space.')


def _load_batch(ckpt_dire, batch, batch_count, expected_files):
    """One checkpoint back, refusing it if it does not describe this run's batch.

    There is no separate finished-marker beside each checkpoint: _write_batch
    renames the file into place, so the name appears only once the contents are
    complete, and a pickle cannot be read back half-written -- it carries a STOP
    opcode and raises without one. The rename is atomic but not durable, so the
    one way to reach a truncated file is a node crash between the rename and the
    data reaching the disk; that lands here rather than in silently short edges.
    """
    path = _batch_path(ckpt_dire, batch, batch_count)
    try:
        with open(path, 'rb') as f:
            payload = pickle.load(f)
    except (EOFError, pickle.UnpicklingError) as exc:
        raise SystemExit(
            f'{path} is truncated ({exc}), which takes a crash between the rename '
            f'that put it there and its contents reaching the disk.\n'
            f'Delete it and run the build again -- that batch is recomputed, the '
            f'rest of {ckpt_dire} is still good.')
    if payload.get('files') != list(expected_files):
        raise SystemExit(
            f'{path} was written for a different set of shards than batch {batch} '
            f'holds now (the citations dump changed under the checkpoints).\n'
            f'Delete {ckpt_dire} and run the build again.')
    return payload['citing2cited']


# single-file worker (runs in a subprocess)
def get_citation(file_path):
    """citing corpusid -> [cited corpusids] for one shard.

    Only this direction is collected; the reverse is derived later from the
    merged result rather than being carried alongside it.
    """
    citing2cited = defaultdict(list)
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            cited_id = obj.get('citedcorpusid', None)
            citing_id = obj.get('citingcorpusid', None)

            if cited_id is None or citing_id is None:
                continue

            citing2cited[citing_id].append(cited_id)

    # a plain dict pickles back to the parent process more cheaply
    return dict(citing2cited)


def build(citations_dire, save_dire, max_workers, batch_count, checkpoint=True):
    ckpt_dire = os.path.join(save_dire, CHECKPOINT_DIRNAME)
    if checkpoint:
        os.makedirs(ckpt_dire, exist_ok=True)
        _warn_about_foreign_checkpoints(ckpt_dire, batch_count)

    citing2cited_M = defaultdict(list)
    batch_M = defaultdict(list)

    def merge_citation(result):
        # extend, not update: one corpusid's edges are spread across shards
        for cid, cid_list in result.items():
            batch_M[cid].extend(cid_list)

    def batch_done(batch, files):
        """A finished batch either goes to disk or straight into the master dict."""
        if checkpoint:
            _write_batch(ckpt_dire, batch, batch_count, files, batch_M)
        else:
            for cid, cid_list in batch_M.items():
                citing2cited_M[cid].extend(cid_list)
        # extend copied the ids out, so dropping this batch's lists is safe and
        # keeps the peak at the master dict plus one batch rather than two
        batch_M.clear()

    def already_done(batch, files):
        return checkpoint and os.path.isfile(_batch_path(ckpt_dire, batch, batch_count))

    batch2file = process_shards(get_citation, citations_dire, merge_citation, "citations",
                                max_workers=max_workers, batch_count=batch_count,
                                skip_batch=already_done, on_batch=batch_done)

    # With checkpoints the master dict is still empty here: every batch went to
    # disk, including ones this run skipped because an earlier run wrote them.
    if checkpoint:
        for batch in tqdm(sorted(batch2file), desc="load batches"):
            for cid, cid_list in _load_batch(ckpt_dire, batch, batch_count,
                                             batch2file[batch]).items():
                citing2cited_M[cid].extend(cid_list)

    edge_count = sum(len(cid_list) for cid_list in citing2cited_M.values())
    citing2cited_M = {cid: list(set(cid_list))
                      for cid, cid_list in tqdm(citing2cited_M.items(), desc="dedup")}
    unique_count = sum(len(cid_list) for cid_list in citing2cited_M.values())

    with open(os.path.join(save_dire, FORWARD_NAME), 'wb') as f:
        pickle.dump(citing2cited_M, f, pickle.HIGHEST_PROTOCOL)

    # only now: until the forward file exists, the checkpoints are the run
    if checkpoint:
        shutil.rmtree(ckpt_dire, ignore_errors=True)

    print("citing corpusids       ", len(citing2cited_M))
    print("edges read             ", edge_count)
    print("edges after dedup      ", unique_count)


def invert(save_dire):
    forward_path = os.path.join(save_dire, FORWARD_NAME)
    if not os.path.isfile(forward_path):
        raise SystemExit(f'{forward_path} does not exist. Run the build step first.')

    with open(forward_path, 'rb') as f:
        citing2cited = pickle.load(f)
    print(f"loaded {len(citing2cited)} citing corpusids from {FORWARD_NAME}")

    # popitem() as we go, so the source shrinks while the reverse grows instead
    # of both being fully resident at once. The forward lists are already
    # deduplicated, so each (citing, cited) pair is seen once and the reverse
    # lists need no second dedup.
    cited2citing = defaultdict(list)
    with tqdm(total=len(citing2cited), desc="invert") as bar:
        while citing2cited:
            citing_id, cited_id_list = citing2cited.popitem()
            for cited_id in cited_id_list:
                cited2citing[cited_id].append(citing_id)
            bar.update(1)

    cited2citing = dict(cited2citing)
    with open(os.path.join(save_dire, REVERSE_NAME), 'wb') as f:
        pickle.dump(cited2citing, f, pickle.HIGHEST_PROTOCOL)

    print("cited corpusids        ", len(cited2citing))
    print("edges                  ", sum(len(v) for v in cited2citing.values()))


if __name__ == "__main__":

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--step', choices=['all', 'build', 'invert'], default='all',
                    help='which step to run (default all)')
    ap.add_argument('--workers', type=int, default=WORKERS,
                    help=f'worker processes for the build step (default {WORKERS}, 1 = in-process)')
    ap.add_argument('--batch-count', type=int, default=BATCH_COUNT,
                    help=f'batches the shards are split into (default {BATCH_COUNT})')
    ap.add_argument('--no-checkpoint', action='store_true',
                    help='do not check finished batches out to disk during build. '
                         'Halves the disk the build needs, at the cost of losing '
                         'the whole run if it is interrupted')
    args = ap.parse_args()

    if args.workers < 1 or args.batch_count < 1:
        raise SystemExit('--workers and --batch-count must be at least 1')

    # resolved here rather than at module level: on Windows every worker
    # subprocess re-imports this file, and only the parent needs the paths
    release_id = get_local_release_id()
    print("release_id:", release_id)

    save_dire = os.path.join(META_DIRE, release_id)
    os.makedirs(save_dire, exist_ok=True)

    if args.step in ('all', 'build'):
        citations_dire = os.path.join(S2AG_DIRE, release_id, 'citations')
        if not os.path.isdir(citations_dire):
            raise SystemExit(f'{citations_dire} does not exist. Run download_s2ag.py first.')
        build(citations_dire, save_dire, args.workers, args.batch_count,
              checkpoint=not args.no_checkpoint)

    if args.step in ('all', 'invert'):
        invert(save_dire)

    print("finished")

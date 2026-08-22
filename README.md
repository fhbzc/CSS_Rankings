# CSS Ranking ([the official website](https://cssrankings.org/))

**A ranking of computational social science faculty, built to make CSS faculty search easier** — for prospective PhD students looking for an advisor, for departments running a search, and for anyone trying to find out who works on a topic and where. CSS is scattered across sociology, political science, information science, communication, computer science and public policy, so the people working on one question are rarely in one directory. This puts them in one place, filterable by research area.

## Updates
On 2026-08-22:
  - Add profiles for 5 faculty

On 2026-08-15:
  - Add profiles for 12 faculty and change affiliation for 1 faculty
  - Support display for only *Junior* faculty (academic age less than 12-years, counting from their first publication)
  - Add region selection based on continent
  - Revise the paper categorization criteria, increasing the proportion of publications by the listed faculty classified as CSS papers from 53% to over 70%. (the current category is based on LLM as a judge which is not ideal, will think about how to make it better)

## Who is on the list

The roster starts from **[CSS_program](https://github.com/fhbzc/CSS_program)**, the first version of this project — a community-curated list of CSS faculty, together with the research categories it defined. That list is the seed and the standard: the 21 areas here are its categories, carried forward.

From there the roster was **expanded with LLM agents**, area by area, briefed on the community list as examples of what belongs and asked to find comparable faculty elsewhere. Every candidate they proposed was then checked against two standing rules:

- **Method bar** — the work must be computational: network analysis, agent-based modelling, machine learning, NLP on text corpora, large-scale digital trace data, simulation, algorithm auditing. Interview and ethnographic work, survey research, small-N regression and pure theory do not count.
- **Employer bar** — the institution must train doctoral students (research institutes that award PhDs count; industry labs do not). Research-track and teaching-track faculty count, not only tenure-track.

Borderline cases were left out rather than in — **precision over recall**. That is a deliberate choice and it is the main reason someone may be missing, so please do tell us when we got it wrong.

## How people are scored

Scores come from the [Semantic Scholar Academic Graph](https://www.semanticscholar.org/product/api) (release `2026-07-28`): every roster member's papers, the venues they appeared in, and the citations between them. Pick a set of **research areas** and a **year window**, and everyone is scored on the papers that fall in both, by two numbers:

**`paper_count`** — how many such papers a person has. A paper is counted **once** however many of the selected areas it belongs to: selecting both *Culture* and *Gender & Race* does not count a paper labelled with both of them twice. This is the plain measure of output in a field.

**`weighted_if`** — the sum of `impact_factor / n_authors` over those same papers. Two things are folded in:

- *where* it was published — `impact_factor` is the venue's impact factor, so a paper in a stronger venue contributes more;
- *how much of it is theirs* — dividing by `n_authors` splits each paper equally among its authors, so a solo paper in a venue with an impact factor of 4 contributes the same 4 as a four-author paper in a venue with an impact factor of 16.

The impact factors are **computed here, from the citation graph** — not taken from Clarivate. For a venue in year Y: the citations made during Y to the papers it published in Y−1 and Y−2, divided by how many papers those were. That is the standard Journal Impact Factor formula, applied to Semantic Scholar's coverage rather than Web of Science, so it covers CS conferences that a JCR number does not exist for. Every paper takes its venue's figure from a single reference year, so the number ranks venues and never separates two papers in the same venue by the year they happened to appear in.

Both are **summed, never averaged**, over whatever the reader selected. That decides how the ranking reads: a prolific researcher outranks a selective one with the same average quality, and a large group outranks a small one. If you want selectivity instead, compare `weighted_if / paper_count` yourself — the exported data supports it.

## Contributing

**The roster is certainly incomplete and may contain mistakes — that is what this repository is for.** If someone is missing, or someone's information is wrong, please tell us — either way is welcome:

- **Open an issue** describing the person and what is wrong or missing. This is the easiest route, and you do not need to know the data format.
- **Send a PR** editing the files directly, both in [`data/`](data/):
  - [`data/faculty.csv`](data/faculty.csv) — who is on the roster: `name`, `affiliation`, `country`, `homepage`
  - [`data/faculty_paper_list_w2016_2025.csv`](data/faculty_paper_list_w2016_2025.csv) — their papers: `name`, `title`, `pub_year`, `venue_name`, `impact_factor`, `n_authors`, plus one column per research area (`1` = the paper belongs to it)

Please say **why** in the issue or PR description — a homepage, a CV, a Google Scholar profile. Corrections to areas are as useful as corrections to people: the area labels are judged from titles and abstracts and they get things wrong.

Both files are **UTF-8 CSV**, one record per line, standard quoting — a field containing a comma is wrapped in `"..."` and an inner quote is doubled (`""`). Plenty of titles, affiliations and area names contain commas, so if you edit in Excel or Google Sheets, **save back as UTF-8 CSV** and the quoting is handled for you. Please do not reorder or rename columns.

## Data

| file | rows | what it is |
| --- | --- | --- |
| [`data/faculty.csv`](data/faculty.csv) | 1,089 | one row per person |
| [`data/faculty_paper_list_w2016_2025.csv`](data/faculty_paper_list_w2016_2025.csv) | 48,410 | one row per (person, paper), 2016–2025 |

A paper co-authored by two people on the roster appears on both of their rows. Papers are labelled across 21 research areas in 6 groups, each column `1` or `0`; a paper can belong to several. `n_areas` is how many, and `area_evidence` is a one-line summary of what the paper is about — the reading the labels were assigned from, so you can see whether a wrong label came from a wrong reading.

## How it is built — [`data_computation/`](data_computation/)

Run in order. All settings live in `meta_config.py`; the steps that talk to the API need `SEMANTIC_SCHOLAR_API_KEY` in the environment.

| # | script | what it does |
| --- | --- | --- |
| 1 | `download_s2ag.py` | Downloads the raw S2AG bulk dumps (papers, citations, authors). Resumable. |
| 2 | `get_s2ag_meta.py` | Extracts per-paper year, venue, title and author ids from the papers dump. |
| 3 | `get_s2ag_citation.py` | Builds the citation network, citing → cited (`--step build` is all this pipeline needs). |
| 4 | `get_valid_venue_s1.py` | Ranks every venue by paper count, for you to read. **You then list the junk venues** — preprint servers, placeholders — in `meta_config.INVALID_VENUE_IDS`. |
| 5 | `compute_venue_impact_factor.py` | Each venue's impact factor per year: citations made during year Y to the venue's papers from Y−1 and Y−2, over how many papers those were. The Journal Impact Factor formula computed over S2AG — *not* the Clarivate JCR number. |
| 6 | `get_faculty_paper_list.py` | Joins the roster, the paper→author credits and the impact factors into one row per (person, paper). Every paper takes its venue's figure from a single reference year, so the number ranks venues rather than years. Papers in an invalid venue are dropped here. |
| 7 | `get_paper_category_s1.py` | Fetches each paper's abstract, TLDR and field tags from the S2 API — the text an area label is judged from. Resumable; a full fetch takes hours. |
| 8 | `get_paper_abstract_backfill.py` | Fills abstracts S2 cannot redistribute from OpenAlex, matched by DOI only. |
| — | *(area labelling)* | The 21 area columns are judged from that text in batches and written back into the paper list. This is the step humans should check, and the one PRs are most useful for. |
| 9 | `export_webpage_data.py` | Aggregates to (person, year, area combination) and writes the files the site reads. Grouping by the *combination* is what keeps a paper in two areas counted once when a reader selects both. |

[`Workpipeline.txt`](data_computation/Workpipeline.txt) documents every step in full — inputs, outputs, and why each choice was made.

### Scope and caveats

- Only papers **published in 2016–2025** are listed. The impact factors behind them reach further back, because the formula needs the venue's own history.
- `n_authors` counts the authors Semantic Scholar resolved to an author id, so it is a **lower bound**, and `weighted_if` therefore runs slightly high — more so for papers with long author lists.
- Impact factor is a property of the **venue**, not of the paper. It says where the work appeared, not how good or how cited that particular paper is.
- A person's papers are found through their Semantic Scholar author ids, which are imperfect: S2 splits one researcher across several ids and merges distinct researchers into one. Missing or foreign papers usually trace back to this.

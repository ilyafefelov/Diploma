# Editorial and methodology review

## Decision

**Major revision before arXiv upload.** The main V2+ retrospective case study is
refereeable, but the safe-switch result cannot carry the title or a headline
contribution. In journal terms, the current version would be reject-and-invite-
resubmission for an applied energy/control venue.

## Strengths

1. A relevant applied question: BESS recommendations are evaluated by decision
   value rather than forecast error alone.
2. A common LP/oracle contour makes raw, strict, V2, and V2+ comparisons
   coherent and auditable.
3. The paper states the dependence structure: 90 profile-date rows correspond
   to 18 market dates and one shared price process per date.
4. The source/read-model/execution boundaries are unusually explicit;
   `market_execution_enabled=false` is preserved.
5. The compact V2+ evidence bundle reconstructs the headline tables exactly.

## Submission blockers

1. **Title-evidence mismatch.** Safe-switch selection is foregrounded despite
   exact mirrored training/evaluation rows and only one effective switch date.
2. **Definitive leakage disclosure missing.** Phrases such as "may use mirrored
   rows" are false by understatement; exact equality was verified.
3. **Lineage conflation.** The RF number 168.1566 UAH is labelled DT/V2+ in the
   defended thesis, README, metrics atlas, and a paper figure.
4. **Incomparable challenger display.** RF 168.16 and HF 158.71 are plotted as if
   they were commensurate model results, although both are in-packet diagnostics
   with different estimators and gates.
5. **Availability statement not yet true.** The paper/evidence tree is untracked
   and absent from the public repository/tag referenced by the manuscript.
6. **Method is not self-contained enough.** V2+ needs compact pseudocode or an
   algorithm table covering candidate generation, prior-only scoring, fallback,
   thresholding, and tie-breaking.

## Required revision

- Retitle around decision-value evaluation and a retrospective Ukrainian DAM
  study.
- Make V2+ the sole central empirical contribution.
- Compress RF, HF, DT prototype, and LAVA into a model-lineage/negative-evidence
  section; remove RF/HF headline numbers from the abstract.
- State exact mirroring and the single switch date wherever the safe-switch
  diagnostic is discussed.
- Remove inferential use of the three identical RF seeds and stored p-value.
- Add a self-contained V2+ algorithm and evidence-contract table.
- Add a defended-thesis erratum without pretending the defended PDF was changed.
- Publish an immutable release containing the exact TeX, ancillary evidence,
  hashes, and reconstruction commands before submitting.

## Category recommendation

Primary `eess.SY`. Omit `cs.LG` from version 1: the learned-selector evidence
does not have a genuine temporally separated evaluation, and the core
contribution is an energy-system evidence/decision pipeline rather than a new ML
method.

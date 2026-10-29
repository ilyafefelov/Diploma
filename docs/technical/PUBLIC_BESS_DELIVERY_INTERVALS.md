# Public BESS delivery-day intervals

OREE PXS labels are one-based chronological delivery-interval ordinals, not
wall-clock hour numbers. A Europe/Kyiv delivery date has 23, 24, or 25 hourly
intervals between its local midnight and the next local midnight. The parser
requires exactly that date's number of labels and prices, with each ordinal
present once; it does not truncate, pad, or accept incomplete days.

The publication code keys observations by UTC instant. Public LP schedule and
forecast point timestamps use ISO Kyiv offsets. On 2026-10-25, ordinals 4 and 5
are `03:00+03:00` and `03:00+02:00`; ordinal 25 is `23:00+02:00`. Both prices
remain distinct through sorting, deduplication, optimization, and serialization.
LP steps remain one elapsed hour, including across DST transitions.

Legacy naive input timestamps mean Kyiv wall time only when unambiguous.
Ambiguous or nonexistent naive timestamps require an explicit offset. Source
completeness checks compare exact instants; off-grid minutes are rejected.
The existing v1 payload fields are retained, with source status reporting
`complete_23_hour_delivery_day`, `complete_24_hour_delivery_day`, or
`complete_25_hour_delivery_day` and offset-bearing delivery timestamps.

The strict similar-day public forecast uses the previous calendar date for
Tuesday–Friday and the previous week's calendar date otherwise, matching local
hour rather than an elapsed 24/168-hour shift. A repeated target hour uses the
matching source fold when available, otherwise the sole first source occurrence.
A normal target hour uses the first occurrence when the source repeats. This is
an explicit forecast assumption, not a modification of observed prices. Missing
source wall hours block the baseline. History retains both source occurrences,
and training cutoffs serialize the actual final UTC instant.
Observed history must be exactly hour-aligned before the local-hour lookup.
Supplied-model generation/cutoff metadata uses UTC for legacy naive values;
explicit metadata offsets preserve their absolute instant. These metadata fields
are separate from naive Kyiv delivery timestamps.

Regression fixture `tests/publication/oree_2026_10_25.json` is the unmodified
read-only response retrieved on 2026-10-28 from
<https://www.oree.com.ua/index.php/PXS/get_pxs_hdata/25.10.2026/DAM/2>.
`tests/publication/test_delivery_day_dst.py` covers all three day lengths,
the actual 25-price vector, repeated-hour identity, LP alignment, forecasting,
malformed/missing intervals, and legacy timestamp rejection.

The social-card renderer scales its plot to the actual interval count and reads
hour labels from the schedule timestamps. DST-day labels include offsets, and
row-count receipts no longer assume a denominator of 24. Renderer tests write
cards only to temporary local directories.

This repair changes no publication schedules, workflow triggers, historical
artifacts, market execution flags, or strategy promotion boundaries. Validation
materialization must use temporary output directories; it does not publish data.

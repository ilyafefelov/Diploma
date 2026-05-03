# Examples

## Example 1: Weekly report update

**User ask**

```text
Онови weekly report за тиждень 2 і додай ризики, які з'явилися після першого демо.
```

**Expected workflow**

1. Read the relevant `docs/syllabus/` files for the current week.
2. Read the existing weekly report and any new artifacts.
3. Separate verified progress from planned work.
4. Update the report in `docs/thesis/weekly-reports/week2/`.
5. Link screenshots, code, docs, and demo materials.

**Expected output path**

- `docs/thesis/weekly-reports/week2/report.md`

## Example 2: Literature review draft

**User ask**

```text
Напиши перший варіант розділу 2 Огляд літератури для диплома.
```

**Expected workflow**

1. Read `docs/syllabus/` requirements for literature review.
2. Read `CONTEXT.md` and available paper references.
3. Group sources thematically instead of listing them flat.
4. Explain strengths, weaknesses, gaps, and why the project chooses its architecture.
5. Mark target DFL elements as planned if they are not implemented yet.

**Expected output path**

- `docs/thesis/chapters/02-literature-review.md`

## Example 3: Presentation script from weekly report

**User ask**

```text
Зроби сценарій презентації для звіту за перший тиждень.
```

**Expected workflow**

1. Read the current weekly report.
2. Extract only evidence-backed points.
3. Build a concise slide structure.
4. Add speaker notes and artifact references.
5. Keep a clear boundary between implemented MVP and research direction.

**Expected output path**

- `docs/thesis/weekly-reports/week1/presentation-script.md`

## Example 4: Tightening an existing report

**User ask**

```text
Зроби коротшу submission version для week 1 report.
```

**Expected workflow**

1. Read the current report.
2. Remove repetition and long explanations.
3. Preserve required sections: completed work, risks, plan, artifacts.
4. Keep academic tone and verified claims.

**Expected output path**

- Update `docs/thesis/weekly-reports/week1/report.md`

## Example 5: Linking weekly evidence to thesis chapters

**User ask**

```text
Перенеси research-частину з weekly report у чернетку розділу пояснювальної записки.
```

**Expected workflow**

1. Read the weekly report and supporting sources.
2. Extract reusable analytical text.
3. Rewrite it as thesis chapter prose instead of weekly progress prose.
4. Keep the weekly report compact and link to the chapter draft if needed.

**Expected output paths**

- Update `docs/thesis/weekly-reports/weekN/report.md`
- Create or update `docs/thesis/chapters/*.md`

## Example 6: What not to do

**Bad output pattern**

- flat list of papers with no analysis
- claiming a planned DFL module is already working
- copying the same long text into both a weekly report and a chapter draft
- ignoring `docs/syllabus/` when deciding what the user must submit

**Better pattern**

- analytical synthesis
- explicit distinction between implemented and planned
- reuse chapter drafts, keep weekly reports concise
- treat syllabus as the source of truth
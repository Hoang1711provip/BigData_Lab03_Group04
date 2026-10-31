# Lab 3 experiment report
Group / run label / author / reviewer:
Group 04/ Run 01/ PHẠM CHÍ DŨNG/


## 1. Upstream handoff
- **Handoff Choice:** Course fallback (`data/handoff/curated.csv`).
- **Input Verification Status:** PASS 100% via `tools/check_inputs.py` (Test H01).
- **Manifest Hash / Contract:** Verified against `trusted_lab3_inputs.json` SHA-256 signatures.
- **Data Handoff Explanation:** Group selected the official course fallback `curated.csv` to ensure standard schema compatibility across Task 1 to Task 5. Total 9,610 handoff rows and 140 incoming physical revisions were validated.
- **Original Raw Data Location:** Raw inputs and upstream artifacts remain isolated in the immutable local source storage (`data/` directory) and are verified prior to Spark ingestion.

## 2. Table semantics and lineage
Record Bronze, Silver and Gold URIs and table versions. Explain a Bronze
receipt, a Silver business-key winner and a Gold site/day aggregation.
Trace one updated row to source object, physical row and SHA-256.
Draw only actual read/write edges; governance metadata are not data carriers.

## 3. Incremental evidence
Report incoming physical records, per-key winners, inserts, updates,
unchanged/stale records, Bronze count and Silver count. Show the replay
and time-travel results. Distinguish logical equivalence from Delta version
identity and Parquet file-byte identity. Explain the invalid-batch policy.

## 4. Analytics
Use the three saved SQL outputs. Explain why observation-weighted mean
is generally different from AVG(site means). Report numerators/denominators
for late fraction and >25 C fraction. Interpret >25 C as a teaching threshold,
not a calibrated physical safety threshold. State residual QA limitations.

## 5. Controlled layout experiment
Record exact dataset version, copy factor, expanded rows, resource settings,
setup times, total table files/bytes, execution plans, all six elapsed times,
medians, and A-median/B-median. Identify PartitionFilters in B's plan.
Do NOT call total table files 'files scanned'. Do NOT call repeated runs cold.
Explain the outcome even if layout B is slower. Synthetic copies are not new
independent observations and must never enter operational Gold.

## 6. Release and accountability
Record the pinned Silver/Gold pair and generating-code hash; validator code
may have its own hash. Attach five release-gate outcomes and the owner's
independent result. List who authored/reviewed each technical contribution.
Do not include credentials, kubeconfig, or restricted raw payloads.

### Technical Contributions:
**Task 1 (Input Integrity & Spark Preflight):** Authored by PHẠM CHÍ DŨNG. Verified 16 artifacts via 'check_inputs.py (H01) and initiated Spark/Delta environment preflight checks (H02, H03, H04), generating 5 core evidence JSON files in `evidence/`.

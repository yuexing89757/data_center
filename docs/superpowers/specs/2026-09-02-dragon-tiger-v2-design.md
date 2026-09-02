# DragonTiger v2 Design

- Status: approved by the project owner on 2026-09-02
- Governing issue: #70
- Governing ADR: ADR-0049
- Domain design: `docs/领域详设-DragonTiger-2026-09-02.md`

This specification replaces the runtime and public-contract scope of Issue #65. The old three
FastAPI/PostgREST contracts are intentionally removed without a compatibility layer. The implementation
retains only transport, Raw lineage, ingestion/quality and scheduler mechanisms that satisfy the new
domain semantics.

The first vertical slice delivers correct provider-neutral facts and replacement reads. The second slice
delivers deterministic objective metrics, as-of seat profiles and Feature/Label separation. Subjective
capital-quality scores and tourist-capital identities remain outside Market Data Center under the project
constitution.

Authoritative field, missing-value, period, identity, API and deletion semantics are defined in the domain
design and ADR; implementations and tests must cite those documents instead of the superseded v1 design.

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

OUTDATED_PATTERNS = [
    (re.compile(r"\b(google\+|google plus|myspace|vine|periscope|hangouts|google reader)\b", re.I), "obsolete platform or product"),
    (re.compile(r"\b(covid-19|pandemic)\b", re.I), "pandemic-era context may be time-bound"),
    (re.compile(r"\b(twitter)\b", re.I), "brand/platform references may be outdated"),
    (re.compile(r"\b(2019|2020|2021)\b", re.I), "older dated context"),
    (re.compile(r"\b(ira|inflation reduction act)\b", re.I), "policy/incentive details may have changed"),
    (re.compile(r"\b(seer\s?\d|seer2|hers|energy star|net zero|solar tax credit|heat pump water heater)\b", re.I), "technology/specs or incentives may have changed"),
    (re.compile(r"\b(election|president|administration|congress|supreme court|ukraine|gaza|russia|israel|tariff)\b", re.I), "political or world-event context may have changed"),
]

NEVER_SURFACE_PATTERNS = [
    (re.compile(r"\b(draft|internal only|do not publish|confidential|embargo|under embargo|sponsor notes|editor notes)\b", re.I), "explicitly internal or embargoed material"),
    (re.compile(r"\bTODO\b|lorem ipsum|TK\b", re.I), "draft placeholder text"),
]

WEIGHT_ONLY_PATTERNS = [
    (re.compile(r"\b(forecast|prediction|outlook|coming soon|will likely|expected to)\b", re.I), "time-sensitive forward-looking framing"),
]


@dataclass
class GovernanceDecision:
    surface_policy: str
    stale: bool
    stale_reasons: List[str]
    governance_note: str | None


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if len(value) == 10:
            return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def assess_document(
    doc: Dict[str, Any],
    now: datetime | None = None,
    stale_after_years: int = 3,
    weight_only_after_years: int = 5,
) -> GovernanceDecision:
    now = now or datetime.now(timezone.utc)
    text = " ".join(
        str(doc.get(k, "")) for k in ["title", "category", "text", "source_path"]
    )
    reasons: List[str] = []
    stale = False
    surface_policy = doc.get("surface_policy") or ("paraphrase" if doc.get("visibility", "public") == "private" else "public")

    for pattern, reason in NEVER_SURFACE_PATTERNS:
        if pattern.search(text):
            reasons.append(reason)
            return GovernanceDecision("blocked", True, reasons, "Blocked from retrieval and response use")

    dt = _parse_date(doc.get("published_at"))
    if dt is not None:
        age_years = (now - dt).days / 365.25
        if age_years >= stale_after_years:
            stale = True
            reasons.append(f"older than {stale_after_years} years")
        if age_years >= weight_only_after_years and doc.get("visibility") == "private":
            surface_policy = "weight_only"

    for pattern, reason in OUTDATED_PATTERNS:
        if pattern.search(text):
            stale = True
            reasons.append(reason)

    for pattern, reason in WEIGHT_ONLY_PATTERNS:
        if pattern.search(text) and doc.get("visibility") == "private":
            reasons.append(reason)
            if surface_policy != "blocked":
                surface_policy = "weight_only"

    if doc.get("visibility") != "private":
        surface_policy = "public"
    elif stale and surface_policy == "paraphrase":
        surface_policy = "weight_only"

    note = None
    if surface_policy == "weight_only":
        note = "May inform internal weighting/background, but should not be paraphrased or attributed directly"
    elif surface_policy == "paraphrase":
        note = "May influence answers with branded editorial-archive attribution"

    return GovernanceDecision(surface_policy, stale, sorted(set(reasons)), note)


def apply_governance(doc: Dict[str, Any]) -> Dict[str, Any]:
    decision = assess_document(doc)
    updated = dict(doc)
    updated["surface_policy"] = decision.surface_policy
    updated["stale"] = decision.stale
    updated["stale_reasons"] = decision.stale_reasons
    updated["governance_note"] = decision.governance_note
    return updated

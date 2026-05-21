r"""Parse CORDIS organization records and resolve Norwegian orgnrs.

Reads organization.csv rows for all programmes, resolves Norwegian
beneficiaries via VAT pattern ``NO\d{9}MVA`` → first 9 digits = orgnr.
Falls back to name matching against an enheter snapshot when VAT is
blank.

Only Norwegian-resolved rows enter the changelog.  All rows (including
non-NO) are stored in the raw snapshot by the collector.

Resolution methods:
    ``vat``         — deterministic regex on vatNumber field
    ``name_exact``  — exact uppercase match against enheter snapshot
    ``unresolved``  — logged to unresolved.parquet, not in changelog
"""

import re
import hashlib
import json


VAT_RE = re.compile(r"^NO(\d{9})MVA$")


def resolve_orgnr(row, enheter_lookup=None):
    """Resolve a CORDIS organization row to a Norwegian 9-digit orgnr.

    Args:
        row: Dict from organization.csv with keys ``vatNumber``,
            ``name``, ``country``.
        enheter_lookup: Optional dict of ``{uppercase_name: orgnr}``
            from the enheter snapshot.  Used as fallback when VAT is
            blank.

    Returns:
        Tuple of ``(orgnr, method)`` where method is one of
        ``"vat"``, ``"name_exact"``, or ``None`` if unresolved.
    """
    vat = (row.get("vatNumber") or "").strip()
    m = VAT_RE.match(vat)
    if m:
        return m.group(1), "vat"

    if row.get("country") != "NO":
        return None, None

    if enheter_lookup:
        name_upper = (row.get("name") or "").strip().upper()
        orgnr = enheter_lookup.get(name_upper)
        if orgnr:
            return orgnr, "name_exact"

    return None, None


def content_hash(row):
    """Compute a stable hash of a CORDIS organization row.

    Tracks fields that, when changed, should produce a ``modified``
    event in the changelog.
    """
    tracked = [
        row.get("role") or "",
        row.get("ecContribution") or "",
        row.get("netEcContribution") or "",
        row.get("totalCost") or "",
        row.get("endOfParticipation") or "",
        row.get("active") or "",
        row.get("name") or "",
        row.get("activityType") or "",
        row.get("SME") or "",
    ]
    return hashlib.sha256("|".join(tracked).encode()).hexdigest()[:16]


TRACKED_FIELDS = [
    "role", "ecContribution", "netEcContribution", "totalCost",
    "endOfParticipation", "active", "name", "activityType", "SME",
]


def parse_organisations(org_rows, programme, enheter_lookup=None):
    """Parse organization rows and resolve Norwegian participants.

    Args:
        org_rows: List of dicts from organization.csv.
        programme: Programme code for metadata.
        enheter_lookup: Optional name→orgnr dict.

    Returns:
        Tuple of ``(resolved_rows, unresolved_rows)`` where each row
        is a dict with orgnr, resolution metadata, and source fields.
    """
    resolved = []
    unresolved = []

    for row in org_rows:
        orgnr, method = resolve_orgnr(row, enheter_lookup)

        entry = {
            "orgnr": orgnr,
            "orgnr_resolution_method": method,
            "programme": programme,
            "projectID": row.get("projectID", ""),
            "projectAcronym": row.get("projectAcronym", ""),
            "organisationID": row.get("organisationID", ""),
            "vatNumber": row.get("vatNumber", ""),
            "name": row.get("name", ""),
            "shortName": row.get("shortName", ""),
            "SME": row.get("SME", ""),
            "activityType": row.get("activityType", ""),
            "country": row.get("country", ""),
            "city": row.get("city", ""),
            "nutsCode": row.get("nutsCode", ""),
            "role": row.get("role", ""),
            "ecContribution": row.get("ecContribution", ""),
            "netEcContribution": row.get("netEcContribution", ""),
            "totalCost": row.get("totalCost", ""),
            "endOfParticipation": row.get("endOfParticipation", ""),
            "active": row.get("active", ""),
            "contentUpdateDate": row.get("contentUpdateDate", ""),
            "content_hash": content_hash(row),
        }

        if orgnr:
            resolved.append(entry)
        elif row.get("country") == "NO":
            unresolved.append(entry)

    return resolved, unresolved

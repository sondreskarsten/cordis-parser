"""Pipeline entrypoint for the CORDIS parser.

Reads raw organization.csv from GCS snapshots, resolves Norwegian
beneficiaries via VAT→orgnr, and emits unified 12-column changelog.

Modes:

* ``daily`` — read the latest snapshot, resolve, run CDC.
* ``bootstrap`` — read latest snapshot, resolve, emit all as new.
* ``check`` — print snapshot metadata without writing.

Environment variables:

======================== ============================================= =================
Variable                 Description                                   Default
======================== ============================================= =================
GCS_BUCKET               GCS bucket                                    sondre_brreg_data
GCS_PREFIX               Path prefix                                   cordis
RUN_MODE                 ``daily``, ``bootstrap``, or ``check``        daily
SNAPSHOT_DATE            Specific snapshot date (default: latest)       (auto)
ENHETER_BUCKET           Bucket for enheter snapshots                  sondre_brreg_data
ENHETER_PREFIX           Prefix for enheter snapshots                  enheter/parsed/v1/state
======================== ============================================= =================
"""

import os
import sys
from datetime import date

from reader import GCSReader
from parser import parse_organisations
from cdc import CordisCDC

GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "cordis")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")
ENHETER_BUCKET = os.environ.get("ENHETER_BUCKET", "sondre_brreg_data")
ENHETER_PREFIX = os.environ.get("ENHETER_PREFIX", "enheter/parsed/v1/state")

PROGRAMMES = ["HORIZON", "h2020", "fp7", "fp6", "fp5", "fp4"]


def load_enheter_lookup(bucket_name, prefix):
    """Load the latest enheter snapshot as a name→orgnr lookup.

    Used as fallback for CORDIS organizations without vatNumber.
    """
    from google.cloud import storage as gcs_lib
    import pyarrow.parquet as pq
    import io

    client = gcs_lib.Client()
    bucket = client.bucket(bucket_name)

    dates = []
    iterator = bucket.list_blobs(prefix=f"{prefix}/", delimiter="/")
    for page in iterator.pages:
        for blob in page:
            name = blob.name.split("/")[-1]
            if name.endswith(".parquet"):
                dates.append(name.replace(".parquet", ""))
    dates.sort()

    if not dates:
        print("  No enheter snapshots found, name-matching disabled", flush=True)
        return {}

    latest = dates[-1]
    path = f"{prefix}/{latest}.parquet"
    print(f"  Loading enheter snapshot: {path}", flush=True)

    blob = bucket.blob(path)
    data = blob.download_as_bytes()
    table = pq.read_table(io.BytesIO(data), columns=["org_nr", "navn"])

    lookup = {}
    org_nrs = table.column("org_nr").to_pylist()
    names = table.column("navn").to_pylist()
    for orgnr, name in zip(org_nrs, names):
        if orgnr and name:
            lookup[name.upper().strip()] = str(orgnr).strip()

    print(f"  Enheter lookup: {len(lookup):,} entries", flush=True)
    return lookup


def main():
    print(f"{'='*60}", flush=True)
    print(f"  cordis-parser — mode: {RUN_MODE}", flush=True)
    print(f"  {date.today().isoformat()}", flush=True)
    print(f"  GCS: gs://{GCS_BUCKET}/{GCS_PREFIX}/", flush=True)
    print(f"{'='*60}", flush=True)

    reader = GCSReader(GCS_BUCKET, GCS_PREFIX)

    snapshot_dates = reader.list_snapshot_dates()
    if not snapshot_dates:
        print("  No snapshots found. Run cordis-collector first.", flush=True)
        sys.exit(1)

    snapshot = SNAPSHOT_DATE if SNAPSHOT_DATE else snapshot_dates[-1]
    print(f"  Using snapshot: {snapshot}", flush=True)

    if RUN_MODE == "check":
        manifest = reader.load_manifest(snapshot)
        if manifest:
            for prog in manifest.get("programmes", []):
                n_files = len(prog.get("files", []))
                print(f"    {prog['programme']}: {prog['status']}, {n_files} files", flush=True)
        return

    enheter_lookup = load_enheter_lookup(ENHETER_BUCKET, ENHETER_PREFIX)

    all_resolved = []
    all_unresolved = []

    for prog in PROGRAMMES:
        org_rows = reader.read_organisations(snapshot, prog)
        if not org_rows:
            print(f"  {prog}: no organization.csv", flush=True)
            continue

        resolved, unresolved = parse_organisations(org_rows, prog, enheter_lookup)
        print(f"  {prog}: {len(org_rows):,} total → {len(resolved):,} resolved, {len(unresolved):,} unresolved NO", flush=True)

        all_resolved.extend(resolved)
        all_unresolved.extend(unresolved)

    print(f"\n  Total: {len(all_resolved):,} resolved, {len(all_unresolved):,} unresolved", flush=True)

    cdc = CordisCDC(GCS_BUCKET, GCS_PREFIX)
    run_mode = "bootstrap" if RUN_MODE == "bootstrap" else "daily"
    stats = cdc.run(all_resolved, all_unresolved, date.today().isoformat(), run_mode=run_mode)

    print(f"\n  CDC: new={stats['new']}, modified={stats['modified']}, "
          f"changelog_rows={stats['changelog_rows']}, pool={stats['pool_size']:,}, "
          f"snapshots={stats['snapshot_size']:,}, unresolved={stats['unresolved']}", flush=True)


if __name__ == "__main__":
    main()

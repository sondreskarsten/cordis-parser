"""Change Data Capture layer for the CORDIS parser.

Compares today's resolved Norwegian participants against a stored
snapshot to detect new and modified participations, then emits
changelog rows in the unified 12-column event schema.

State files on GCS::

    gs://{bucket}/{prefix}/
    ├── cdc/
    │   ├── pool.parquet          — all Norwegian orgnrs ever seen
    │   ├── snapshots.parquet     — one row per (projectID, organisationID)
    │   └── changelog/
    │       └── YYYY-MM-DD.parquet — daily events in unified 12-col schema
    └── unresolved/
        └── YYYY-MM-DD.parquet    — Norwegian orgs that couldn't be resolved

LUAS: ``(projectID, organisationID)`` — one participation per project
per organisation.  After resolution: ``(projectID, orgnr)``.
"""

import io
import json
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage as gcs_lib


TRACKED_FIELDS = [
    "role", "ecContribution", "netEcContribution", "totalCost",
    "endOfParticipation", "active", "name", "activityType", "SME",
]

SNAPSHOT_SCHEMA = pa.schema([
    ("projectID", pa.string()),
    ("organisationID", pa.string()),
    ("orgnr", pa.string()),
    ("programme", pa.string()),
    ("content_hash", pa.string()),
    ("projectAcronym", pa.string()),
] + [(f, pa.string()) for f in TRACKED_FIELDS])

POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
    ("n_participations", pa.int32()),
    ("programmes", pa.string()),
])

CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("document_id", pa.string()),
    ("data_source", pa.string()),
    ("event_type", pa.string()),
    ("event_subtype", pa.string()),
    ("summary", pa.string()),
    ("changed_fields", pa.string()),
    ("valid_time", pa.string()),
    ("detected_time", pa.string()),
    ("details_json", pa.string()),
    ("source_run_mode", pa.string()),
    ("run_id", pa.string()),
])

UNRESOLVED_SCHEMA = pa.schema([
    ("programme", pa.string()),
    ("projectID", pa.string()),
    ("projectAcronym", pa.string()),
    ("organisationID", pa.string()),
    ("vatNumber", pa.string()),
    ("name", pa.string()),
    ("country", pa.string()),
    ("role", pa.string()),
    ("ecContribution", pa.string()),
])


class CordisCDC:

    def __init__(self, bucket_name, prefix="cordis"):
        self._client = gcs_lib.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.rstrip("/")

    def _gcs_path(self, *parts):
        return "/".join([self._prefix] + list(parts))

    def _read_parquet(self, path):
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        data = blob.download_as_bytes()
        return pq.read_table(io.BytesIO(data))

    def _write_parquet(self, table, path):
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        buf.seek(0)
        blob = self._bucket.blob(path)
        blob.upload_from_file(buf, content_type="application/octet-stream")

    def _list_parsed_dates(self):
        prefix = self._gcs_path("parsed") + "/"
        dates = set()
        iterator = self._bucket.list_blobs(prefix=prefix)
        for blob in iterator:
            name = blob.name.split("/")[-1]
            if name.endswith(".parquet"):
                dates.add(name.replace(".parquet", ""))
        return sorted(dates)

    def _load_previous_parsed(self, run_date):
        dates = [d for d in self._list_parsed_dates() if d < run_date]
        if not dates:
            return {}
        prev = dates[-1]
        t = self._read_parquet(self._gcs_path("parsed", f"{prev}.parquet"))
        if t is None:
            return {}
        d = t.to_pydict()
        result = {}
        for i in range(t.num_rows):
            key = (d["projectID"][i], d["organisationID"][i])
            result[key] = {"content_hash": d["content_hash"][i]}
            for f in TRACKED_FIELDS:
                if f in d:
                    result[key][f] = d[f][i]
        return result

    def _load_pool(self):
        t = self._read_parquet(self._gcs_path("cdc", "pool.parquet"))
        if t is None:
            return {}
        d = t.to_pydict()
        return {d["orgnr"][i]: {
            "first_seen": d["first_seen"][i],
            "last_seen": d["last_seen"][i],
            "n_participations": d["n_participations"][i],
            "programmes": d["programmes"][i],
        } for i in range(t.num_rows)}

    def run(self, resolved_rows, unresolved_rows, run_date, run_mode="daily"):
        """Run CDC against resolved Norwegian participations.

        Args:
            resolved_rows: List of dicts from parser with ``orgnr`` populated.
            unresolved_rows: List of dicts for Norwegian orgs without orgnr.
            run_date: ``yyyy-mm-dd`` string.
            run_mode: ``"daily"`` or ``"bootstrap"``.

        Returns:
            Stats dict.
        """
        run_id = str(uuid.uuid4())[:8]
        detected_time = datetime.now(timezone.utc).isoformat()

        old_snaps = self._load_previous_parsed(run_date)
        pool = self._load_pool()

        changelog_rows = []
        new_count = 0
        mod_count = 0
        new_snaps = {}

        for row in resolved_rows:
            key = (row["projectID"], row["organisationID"])
            h = row["content_hash"]
            snap_row = {
                "projectID": row["projectID"],
                "organisationID": row["organisationID"],
                "orgnr": row["orgnr"],
                "programme": row["programme"],
                "content_hash": h,
                "projectAcronym": row["projectAcronym"],
            }
            for f in TRACKED_FIELDS:
                snap_row[f] = str(row.get(f) or "")
            new_snaps[key] = snap_row

            old_entry = old_snaps.get(key)

            if run_mode == "bootstrap" or old_entry is None:
                event_type = "new"
                changed_fields = None
                new_count += 1
            elif old_entry["content_hash"] != h:
                event_type = "modified"
                diffs = [f for f in TRACKED_FIELDS if str(row.get(f) or "") != str(old_entry.get(f) or "")]
                changed_fields = json.dumps(diffs) if diffs else json.dumps(["content_hash"])
                mod_count += 1
            else:
                continue

            ec = row.get("ecContribution", "")
            summary_parts = [
                row["role"],
                f"{row['programme']} {row['projectAcronym']} ({row['projectID']})",
            ]
            if ec:
                summary_parts.append(f"EUR {ec}")
            summary = " — ".join(summary_parts)

            details = {
                "projectID": row["projectID"],
                "projectAcronym": row["projectAcronym"],
                "organisationID": row["organisationID"],
                "programme": row["programme"],
                "role": row["role"],
                "ecContribution": row.get("ecContribution"),
                "netEcContribution": row.get("netEcContribution"),
                "totalCost": row.get("totalCost"),
                "SME": row.get("SME"),
                "activityType": row.get("activityType"),
                "name": row.get("name"),
                "vatNumber": row.get("vatNumber"),
                "orgnr_resolution_method": row.get("orgnr_resolution_method"),
                "contentUpdateDate": row.get("contentUpdateDate"),
            }

            changelog_rows.append({
                "orgnr": row["orgnr"],
                "document_id": f"{row['projectID']}-{row['organisationID']}",
                "data_source": "cordis",
                "event_type": event_type,
                "event_subtype": f"{row['programme']}_{row['role']}",
                "summary": summary,
                "changed_fields": changed_fields,
                "valid_time": row.get("contentUpdateDate", run_date),
                "detected_time": detected_time,
                "details_json": json.dumps(details, ensure_ascii=False),
                "source_run_mode": run_mode,
                "run_id": run_id,
            })

            orgnr = row["orgnr"]
            prog = row["programme"]
            if orgnr in pool:
                pool[orgnr]["last_seen"] = run_date
                pool[orgnr]["n_participations"] += 1
                existing = set(pool[orgnr]["programmes"].split(",")) if pool[orgnr]["programmes"] else set()
                existing.add(prog)
                pool[orgnr]["programmes"] = ",".join(sorted(existing))
            else:
                pool[orgnr] = {
                    "first_seen": run_date,
                    "last_seen": run_date,
                    "n_participations": 1,
                    "programmes": prog,
                }

        if run_mode != "bootstrap":
            for key, old_h in old_snaps.items():
                if key not in new_snaps:
                    changelog_rows.append({
                        "orgnr": "", "document_id": f"{key[0]}-{key[1]}",
                        "data_source": "cordis", "event_type": "disappeared",
                        "event_subtype": "cordis_participation_ended",
                        "summary": f"Participation ended: project {key[0]}",
                        "changed_fields": None, "valid_time": run_date, "detected_time": detected_time,
                        "details_json": None, "source_run_mode": run_mode, "run_id": run_id,
                    })

        if changelog_rows:
            cl_table = pa.Table.from_pylist(changelog_rows, schema=CHANGELOG_SCHEMA)
            cl_path = self._gcs_path("cdc", "changelog", f"{run_date}.parquet")
            self._write_parquet(cl_table, cl_path)

        snap_rows = list(new_snaps.values())
        if snap_rows:
            snap_table = pa.Table.from_pylist(snap_rows, schema=SNAPSHOT_SCHEMA)
            self._write_parquet(snap_table, self._gcs_path("parsed", f"{run_date}.parquet"))

        pool_rows = [{"orgnr": k, **v} for k, v in pool.items()]
        if pool_rows:
            pool_table = pa.Table.from_pylist(pool_rows, schema=POOL_SCHEMA)
            self._write_parquet(pool_table, self._gcs_path("cdc", "pool.parquet"))

        if unresolved_rows:
            ur_rows = [{k: r.get(k, "") for k in [
                "programme", "projectID", "projectAcronym", "organisationID",
                "vatNumber", "name", "country", "role", "ecContribution"
            ]} for r in unresolved_rows]
            ur_table = pa.Table.from_pylist(ur_rows, schema=UNRESOLVED_SCHEMA)
            self._write_parquet(ur_table, self._gcs_path("unresolved", f"{run_date}.parquet"))

        return {
            "new": new_count,
            "modified": mod_count,
            "changelog_rows": len(changelog_rows),
            "pool_size": len(pool),
            "snapshot_size": len(new_snaps),
            "unresolved": len(unresolved_rows),
        }

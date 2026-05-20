"""GCS reader for the CORDIS parser.

Reads organization.csv and project.csv from versioned snapshots at
``raw/{snapshot_date}/{programme}/``.  Returns parsed dicts for
downstream processing.
"""

import csv
import io
import json

from google.cloud import storage as gcs_lib


class GCSReader:
    """Reads CORDIS raw CSV data from GCS.

    Args:
        bucket_name: GCS bucket name.
        prefix: Path prefix.  Default ``"cordis"``.
    """

    def __init__(self, bucket_name, prefix="cordis"):
        self._client = gcs_lib.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.rstrip("/")

    def list_snapshot_dates(self):
        prefix = f"{self._prefix}/raw/"
        dates = set()
        iterator = self._bucket.list_blobs(prefix=prefix, delimiter="/")
        for page in iterator.pages:
            for p in page.prefixes:
                date_part = p.rstrip("/").split("/")[-1]
                if len(date_part) == 10:
                    dates.add(date_part)
        return sorted(dates)

    def load_manifest(self, snapshot_date):
        path = f"{self._prefix}/raw/{snapshot_date}/manifest.json"
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())

    def read_csv(self, snapshot_date, programme, filename):
        """Read a CSV file and return list of dicts.

        Args:
            snapshot_date: ``yyyy-mm-dd`` string.
            programme: Programme code.
            filename: CSV filename.

        Returns:
            List of row dicts, or empty list if file doesn't exist.
        """
        path = f"{self._prefix}/raw/{snapshot_date}/{programme}/{filename}"
        blob = self._bucket.blob(path)
        if not blob.exists():
            return []
        text = blob.download_as_text(encoding="utf-8")
        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        return list(reader)

    def read_organisations(self, snapshot_date, programme):
        return self.read_csv(snapshot_date, programme, "organization.csv")

    def read_projects(self, snapshot_date, programme):
        return self.read_csv(snapshot_date, programme, "project.csv")

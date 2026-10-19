"""Automated Code Version Tracker & Provenance Auditor for Airflow Pipelines.

Monitors pipeline source files, detects code modifications, automatically bumps
Minor versions (v1.0.0 -> v1.1.0 -> v1.2.0), records diff/lines changed into
PostgreSQL `code_version_audit_logs`, and supplies the exact `source` tag.
"""

import difflib
import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import psycopg2

LOGGER = logging.getLogger(__name__)

# Cache directory for storing previous code snapshots to calculate diffs
CACHE_DIR = Path(__file__).resolve().parent.parent.parent.parent / ".version_cache"


class CodeVersionTracker:
    def __init__(
        self,
        pg_host: Optional[str] = None,
        pg_port: Optional[int] = None,
        pg_user: Optional[str] = None,
        pg_pass: Optional[str] = None,
        pg_db: Optional[str] = None,
    ):
        self.pg_host = pg_host or os.getenv("POSTGRES_HOST", "127.0.0.1")
        self.pg_port = pg_port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.pg_user = pg_user or os.getenv("POSTGRES_USER", "admin123")
        self.pg_pass = pg_pass or os.getenv("POSTGRES_PASSWORD", "admin123")
        self.pg_db = pg_db or os.getenv("POSTGRES_DB", "enterprise_metadata")
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _get_pg_conn(self):
        return psycopg2.connect(
            host=self.pg_host,
            port=self.pg_port,
            user=self.pg_user,
            password=self.pg_pass,
            dbname=self.pg_db,
        )

    @staticmethod
    def compute_file_hash(file_path: str) -> Tuple[str, str]:
        """Reads file content and computes its SHA-256 hash."""
        p = Path(file_path).resolve()
        if not p.exists():
            return "", ""
        with open(p, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
        return content, sha256

    def _get_latest_audit(self, component: str, metric_type: str) -> Optional[dict]:
        """Fetches the latest recorded version for component & metric_type."""
        query = """
            SELECT audit_id, version, component, metric_type, source_tag, file_path, code_hash, created_at
            FROM public.code_version_audit_logs
            WHERE component = %s AND metric_type = %s
            ORDER BY audit_id DESC
            LIMIT 1;
        """
        try:
            with self._get_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (component, metric_type))
                    row = cur.fetchone()
                    if row:
                        return {
                            "audit_id": row[0],
                            "version": row[1],
                            "component": row[2],
                            "metric_type": row[3],
                            "source_tag": row[4],
                            "file_path": row[5],
                            "code_hash": row[6],
                            "created_at": row[7],
                        }
        except Exception as exc:
            LOGGER.warning("Could not query code_version_audit_logs: %s", exc)
        return None

    def _bump_minor_version(self, current_version: str) -> str:
        """Bumps version by Minor increment: v1.0.0 -> v1.1.0 -> v1.2.0."""
        match = re.match(r"^v?(\d+)\.(\d+)\.(\d+)$", current_version.strip())
        if match:
            major, minor, patch = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return f"v{major}.{minor + 1}.0"
        return "v1.1.0"

    def _calculate_diff(self, old_content: str, new_content: str, filename: str) -> Tuple[str, str]:
        """Calculates unified diff and summary of modified lines."""
        old_lines = old_content.splitlines(keepends=True)
        new_lines = new_content.splitlines(keepends=True)

        diff = list(
            difflib.unified_diff(
                old_lines,
                new_lines,
                fromfile=f"a/{filename} (previous)",
                tofile=f"b/{filename} (current)",
                n=3,
            )
        )

        added = sum(1 for line in diff if line.startswith("+") and not line.startswith("+++"))
        removed = sum(1 for line in diff if line.startswith("-") and not line.startswith("---"))

        # Find line numbers from hunk headers @@ -l,s +l,s @@
        hunk_ranges = []
        for line in diff:
            if line.startswith("@@"):
                hunk_match = re.search(r"@@ -(\d+(?:,\d+)?) \+(\d+(?:,\d+)?) @@", line)
                if hunk_match:
                    hunk_ranges.append(f"+{hunk_match.group(2)}")

        range_desc = ", ".join(hunk_ranges[:3]) if hunk_ranges else "File content"
        lines_modified = f"{range_desc} modified (+{added}, -{removed} lines)"
        diff_summary = "".join(diff[:100])  # store first 100 diff lines
        return lines_modified, diff_summary

    def resolve_and_audit_source(
        self,
        file_path: str,
        component: str = "api",
        metric_type: str = "klines",
    ) -> str:
        """
        Resolves the source tag, automatically detecting file changes,
        bumping Minor version, and saving change audit logs.
        """
        content, current_hash = self.compute_file_hash(file_path)
        if not content:
            LOGGER.warning("File %s does not exist or is empty; defaulting to v1.0.0", file_path)
            return f"v1.0.0_{component}_{metric_type}"

        rel_path = os.path.relpath(file_path, os.getenv("GRADUATION_WORKSPACE", "e:/Graduation_Thesis"))
        filename = Path(file_path).name
        snapshot_file = CACHE_DIR / f"{component}_{metric_type}_{filename}.snapshot"

        latest = self._get_latest_audit(component, metric_type)

        # 1. First time baseline registration
        if not latest:
            version = "v1.0.0"
            source_tag = f"{version}_{component}_{metric_type}"
            lines_modified = "Initial Baseline Registration"
            diff_summary = f"Registered initial baseline for {filename}"

            self._record_audit(
                version=version,
                component=component,
                metric_type=metric_type,
                source_tag=source_tag,
                file_path=rel_path,
                lines_modified=lines_modified,
                diff_summary=diff_summary,
                code_hash=current_hash,
            )
            # Save snapshot
            snapshot_file.write_text(content, encoding="utf-8")
            LOGGER.info("Registered initial code version: %s for %s", source_tag, filename)
            return source_tag

        # 2. File unchanged -> reuse active version
        if latest["code_hash"] == current_hash:
            LOGGER.debug("Code unchanged (%s). Active version: %s", filename, latest["source_tag"])
            return latest["source_tag"]

        # 3. File modified -> Bump minor version and record diff!
        old_content = ""
        if snapshot_file.exists():
            old_content = snapshot_file.read_text(encoding="utf-8", errors="replace")

        lines_modified, diff_summary = self._calculate_diff(old_content, content, filename)
        new_version = self._bump_minor_version(latest["version"])
        new_source_tag = f"{new_version}_{component}_{metric_type}"

        self._record_audit(
            version=new_version,
            component=component,
            metric_type=metric_type,
            source_tag=new_source_tag,
            file_path=rel_path,
            lines_modified=lines_modified,
            diff_summary=diff_summary,
            code_hash=current_hash,
        )
        # Update snapshot cache
        snapshot_file.write_text(content, encoding="utf-8")
        LOGGER.info(
            "Auto-bumped code version to %s for %s (%s)",
            new_source_tag,
            filename,
            lines_modified,
        )
        return new_source_tag

    def _record_audit(
        self,
        version: str,
        component: str,
        metric_type: str,
        source_tag: str,
        file_path: str,
        lines_modified: str,
        diff_summary: str,
        code_hash: str,
    ):
        """Inserts an audit record into PostgreSQL."""
        insert_sql = """
            INSERT INTO public.code_version_audit_logs
            (version, component, metric_type, source_tag, file_path, lines_modified, diff_summary, code_hash, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        try:
            with self._get_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        insert_sql,
                        (
                            version,
                            component,
                            metric_type,
                            source_tag,
                            file_path,
                            lines_modified,
                            diff_summary,
                            code_hash,
                            datetime.now(timezone.utc),
                        ),
                    )
                conn.commit()
        except Exception as exc:
            LOGGER.error("Failed to write to code_version_audit_logs: %s", exc)


# Singleton helper
tracker = CodeVersionTracker()

def get_pipeline_source_tag(file_path: str, component: str = "api", metric_type: str = "klines") -> str:
    """Convenience helper for Airflow DAGs to get the dynamic source tag."""
    return tracker.resolve_and_audit_source(file_path, component, metric_type)

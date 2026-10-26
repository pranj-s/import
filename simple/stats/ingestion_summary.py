# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Builds the per-run ingestion summary report (ingestion_report.json).

One report is written per preprocessor run, next to the run's JSON-LD output.
The shape follows the v0 report in "Data Validation in DCP Ingestion":

  {
    "run": {...},
    "folders": [
      {"folderName": ..., "summary": {...}, "provenances": [{...}]}
    ],
    "errors": [...]
  }

Only fields the parent process already knows are filled in for now: run
metadata, input file counts and per-provenance file counts. Observation, date
range and node counts come in follow-up changes. Until then they are left out
of the report rather than reported as zero, so a missing count is never read
as "no data".
"""

from collections import Counter
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
import threading
from typing import Callable, Optional

RUN_MODE_INGEST = "INGEST"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILURE = "FAILURE"

_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _utc_now() -> str:
  return datetime.now(timezone.utc).strftime(_TIME_FORMAT)


@dataclass
class _ProvenanceSummary:
  file_count: int = 0


@dataclass
class _FolderSummary:
  files_by_type: Counter = field(default_factory=Counter)
  provenances: dict[str, _ProvenanceSummary] = field(default_factory=dict)


class IngestionSummary:
  """Accumulates per-run counts and renders the ingestion report.

  This class knows nothing about Config, Runner or the file system: callers
  record facts, and to_json() renders them. That keeps it unit-testable and
  lets later changes add counters without touching the call sites.
  """

  def __init__(self, folder_names: list[str]) -> None:
    """
    Args:
      folder_names: The import folders requested for this run, in the order
        they should be reported. Each one appears in the report even if no
        files matched in it.
    """
    self._lock = threading.Lock()
    self._folder_names = list(folder_names)
    self._start_time = _utc_now()
    self._folders: dict[str, _FolderSummary] = {}

  def record_input_file(self, folder_name: str, file_type: str,
                        provenance: Optional[str]) -> None:
    """Records one input file that matched the config and will be imported.

    Args:
      folder_name: The import folder the file belongs to.
      file_type: The lowercase file extension, e.g. "csv" or "mcf".
      provenance: The provenance configured for the file, or None if the
        config doesn't set one.
    """
    with self._lock:
      folder = self._folders.setdefault(folder_name, _FolderSummary())
      folder.files_by_type[file_type] += 1
      if provenance:
        folder.provenances.setdefault(provenance,
                                      _ProvenanceSummary()).file_count += 1

  def to_json(self,
              status: str,
              output_path: str,
              workflow_execution_id: Optional[str] = None,
              source_id_for: Callable[[str], Optional[str]] = lambda _: None,
              errors: Optional[list[dict]] = None) -> dict:
    """Renders the report.

    Args:
      status: STATUS_SUCCESS or STATUS_FAILURE.
      output_path: Where this run's JSON-LD output was written.
      workflow_execution_id: The ingestion workflow execution ID, if any.
      source_id_for: Returns the source ID for a provenance, or None if it
        isn't known.
      errors: Run-level errors, as JSON-serializable dicts.
    """
    with self._lock:
      return {
          "run": {
              "workflowExecutionId": workflow_execution_id,
              "mode": RUN_MODE_INGEST,
              "status": status,
              "startTime": self._start_time,
              "endTime": _utc_now(),
              "folderNames": list(self._folder_names),
              "outputPath": output_path,
          },
          "folders": [
              self._folder_json(name, source_id_for)
              for name in self._ordered_folder_names()
          ],
          "errors": list(errors or []),
      }

  def _ordered_folder_names(self) -> list[str]:
    """Requested folders first, in order, then any others found, sorted."""
    extra = sorted(set(self._folders) - set(self._folder_names))
    return self._folder_names + extra

  def _folder_json(self, name: str,
                   source_id_for: Callable[[str], Optional[str]]) -> dict:
    folder = self._folders.get(name, _FolderSummary())
    return {
        "folderName":
            name,
        "summary": {
            "files": {
                "count": sum(folder.files_by_type.values()),
                "byType": dict(sorted(folder.files_by_type.items())),
            },
            "provenanceCount": len(folder.provenances),
        },
        "provenances": [{
            "id": prov_id,
            "sourceId": source_id_for(prov_id),
            "fileCount": prov.file_count,
        } for prov_id, prov in sorted(folder.provenances.items())],
    }

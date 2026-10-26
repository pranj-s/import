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

import unittest

from freezegun import freeze_time
from stats.ingestion_summary import IngestionSummary
from stats.ingestion_summary import STATUS_FAILURE
from stats.ingestion_summary import STATUS_SUCCESS


class TestIngestionSummary(unittest.TestCase):

  def test_run_block(self):
    with freeze_time("2026-10-20T15:00:00Z"):
      summary = IngestionSummary(["oecd_wages"])
    with freeze_time("2026-10-20T15:03:12Z"):
      report = summary.to_json(status=STATUS_SUCCESS,
                               output_path="gs://bucket/jsonld/run",
                               workflow_execution_id="4b1c")

    self.assertEqual(
        report["run"], {
            "workflowExecutionId": "4b1c",
            "mode": "INGEST",
            "status": "SUCCESS",
            "startTime": "2026-10-20T15:00:00Z",
            "endTime": "2026-10-20T15:03:12Z",
            "folderNames": ["oecd_wages"],
            "outputPath": "gs://bucket/jsonld/run",
        })
    self.assertEqual(report["errors"], [])

  def test_files_and_provenances(self):
    summary = IngestionSummary(["oecd_wages"])
    summary.record_input_file("oecd_wages", "csv", "dcid:OECDWages")
    summary.record_input_file("oecd_wages", "csv", "dcid:OECDGenderWageGap")
    summary.record_input_file("oecd_wages", "mcf", "dcid:OECDWages")
    summary.record_input_file("oecd_wages", "mcf", None)

    report = summary.to_json(status=STATUS_SUCCESS,
                             output_path="out",
                             source_id_for=lambda prov:
                             {"dcid:OECDWages": "dcid:dc/s/oecd"}.get(prov))

    self.assertEqual(report["folders"], [{
        "folderName":
            "oecd_wages",
        "summary": {
            "files": {
                "count": 4,
                "byType": {
                    "csv": 2,
                    "mcf": 2
                }
            },
            "provenanceCount": 2,
        },
        "provenances": [{
            "id": "dcid:OECDGenderWageGap",
            "sourceId": None,
            "fileCount": 1
        }, {
            "id": "dcid:OECDWages",
            "sourceId": "dcid:dc/s/oecd",
            "fileCount": 2
        }],
    }])

  def test_requested_folder_with_no_files_is_reported(self):
    summary = IngestionSummary(["b_folder", "a_folder"])
    summary.record_input_file("b_folder", "csv", "dcid:P")

    report = summary.to_json(status=STATUS_SUCCESS, output_path="out")

    self.assertEqual([f["folderName"] for f in report["folders"]],
                     ["b_folder", "a_folder"])
    self.assertEqual(report["folders"][1]["summary"], {
        "files": {
            "count": 0,
            "byType": {}
        },
        "provenanceCount": 0,
    })
    self.assertEqual(report["folders"][1]["provenances"], [])

  def test_unrequested_folders_are_appended_sorted(self):
    summary = IngestionSummary(["main"])
    summary.record_input_file("zeta", "csv", None)
    summary.record_input_file("alpha", "csv", None)

    report = summary.to_json(status=STATUS_SUCCESS, output_path="out")

    self.assertEqual([f["folderName"] for f in report["folders"]],
                     ["main", "alpha", "zeta"])

  def test_failure_with_errors(self):
    summary = IngestionSummary(["f"])
    errors = [{"file": "a.csv", "errorType": "UNMAPPED_COLUMNS"}]

    report = summary.to_json(status=STATUS_FAILURE,
                             output_path="out",
                             errors=errors)

    self.assertEqual(report["run"]["status"], "FAILURE")
    self.assertIsNone(report["run"]["workflowExecutionId"])
    self.assertEqual(report["errors"], errors)

  def test_unimplemented_counts_are_omitted(self):
    summary = IngestionSummary(["f"])
    summary.record_input_file("f", "csv", "dcid:P")

    report = summary.to_json(status=STATUS_SUCCESS, output_path="out")

    folder = report["folders"][0]
    self.assertNotIn("observations", folder["summary"])
    self.assertNotIn("nodes", folder["summary"])
    for key in ("observations", "dateRange", "nodes"):
      self.assertNotIn(key, folder["provenances"][0])


if __name__ == "__main__":
  unittest.main()

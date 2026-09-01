#!/usr/bin/env python3
"""Create a cloud-safe Apple Health export before data enters a free workspace.

The script intentionally runs outside Databricks. It keeps selected analytical
measurements, removes direct identifiers and sensitive record families,
pseudonymizes sources, normalizes timestamps to UTC, and shifts every timestamp
by the same undisclosed number of whole weeks so temporal relationships remain
usable without exposing the original dates.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import secrets
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree as ET


ALLOWED_TYPES = {
    "HKQuantityTypeIdentifierActiveEnergyBurned",
    "HKQuantityTypeIdentifierAppleWalkingSteadiness",
    "HKQuantityTypeIdentifierBasalEnergyBurned",
    "HKQuantityTypeIdentifierDistanceWalkingRunning",
    "HKQuantityTypeIdentifierFlightsClimbed",
    "HKQuantityTypeIdentifierHeadphoneAudioExposure",
    "HKQuantityTypeIdentifierStepCount",
    "HKQuantityTypeIdentifierWalkingAsymmetryPercentage",
    "HKQuantityTypeIdentifierWalkingDoubleSupportPercentage",
    "HKQuantityTypeIdentifierWalkingSpeed",
    "HKQuantityTypeIdentifierWalkingStepLength",
    "HKCategoryTypeIdentifierHeadphoneAudioExposureEvent",
    "HKCategoryTypeIdentifierSleepAnalysis",
    "HKDataTypeSleepDurationGoal",
}

KEPT_ATTRIBUTES = {
    "type",
    "creationDate",
    "startDate",
    "endDate",
    "value",
    "unit",
}


def parse_apple_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S %z")


def protected_timestamp(value: str, shift_days: int) -> str:
    timestamp = parse_apple_timestamp(value).astimezone(timezone.utc)
    shifted = timestamp + timedelta(days=shift_days)
    return shifted.strftime("%Y-%m-%d %H:%M:%S %z")


def pseudonymous_source(record_type: str) -> str:
    if "Headphone" in record_type:
        return "protected_headphone_sensor"
    if "Sleep" in record_type:
        return "protected_sleep_source"
    return "protected_iphone_sensor"


def deidentify_record(element: ET.Element, shift_days: int) -> ET.Element:
    record_type = element.attrib["type"]
    attributes = {
        key: value
        for key, value in element.attrib.items()
        if key in KEPT_ATTRIBUTES
    }
    attributes["sourceName"] = pseudonymous_source(record_type)
    attributes["sourceVersion"] = "protected"
    for key in ("creationDate", "startDate", "endDate"):
        if key in attributes:
            attributes[key] = protected_timestamp(attributes[key], shift_days)
    # No device attribute or MetadataEntry children are copied.
    return ET.Element("Record", attributes)


def find_export_member(archive: zipfile.ZipFile) -> str:
    matches = [
        name for name in archive.namelist()
        if name == "export.xml" or name.endswith("/export.xml")
    ]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one export.xml; found {len(matches)}")
    return matches[0]


def create_protected_export(input_zip: Path, output_zip: Path) -> dict[str, object]:
    # A whole-week shift preserves weekday patterns. The actual offset is not
    # written to the output, logs, manifest, or source control.
    shift_days = -7 * secrets.choice(range(104, 261))
    included: collections.Counter[str] = collections.Counter()
    excluded_count = 0
    invalid_timestamp_count = 0

    with zipfile.ZipFile(input_zip, "r") as source_archive:
        export_member = find_export_member(source_archive)
        with source_archive.open(export_member, "r") as source_xml, zipfile.ZipFile(
            output_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as target_archive:
            with target_archive.open("apple_health_export/export.xml", "w") as target_xml:
                target_xml.write(
                    b'<?xml version="1.0" encoding="UTF-8"?>\n'
                    b'<HealthData locale="en_US" privacyStatus="deidentified">\n'
                )
                for _event, element in ET.iterparse(source_xml, events=("end",)):
                    if element.tag.rsplit("}", 1)[-1] != "Record":
                        continue
                    record_type = element.attrib.get("type")
                    if record_type not in ALLOWED_TYPES:
                        excluded_count += 1
                        element.clear()
                        continue
                    try:
                        protected = deidentify_record(element, shift_days)
                    except (KeyError, ValueError):
                        invalid_timestamp_count += 1
                        element.clear()
                        continue
                    target_xml.write(b"  ")
                    target_xml.write(ET.tostring(protected, encoding="utf-8"))
                    target_xml.write(b"\n")
                    included[record_type] += 1
                    element.clear()
                target_xml.write(b"</HealthData>\n")

            manifest = {
                "artifact": "code-deidentified Apple Health analytical export",
                "deidentified": True,
                "contains_original_measurement_values": True,
                "transformations": [
                    "removed personal profile and CDA/clinical document",
                    "excluded sensitive record families and body measurements",
                    "removed device attributes and nested metadata",
                    "pseudonymized source names and versions",
                    "normalized timestamps to UTC",
                    "shifted all timestamps by one undisclosed whole-week offset",
                ],
                "included_record_count": sum(included.values()),
                "excluded_record_count": excluded_count,
                "invalid_timestamp_count": invalid_timestamp_count,
                "included_types": dict(sorted(included.items())),
                "residual_privacy_notice": (
                    "Measurement values and temporal relationships are preserved. "
                    "Treat this file as private analytical data and never commit it to Git."
                ),
            }
            target_archive.writestr(
                "PRIVACY_MANIFEST.json",
                json.dumps(manifest, indent=2) + "\n",
            )
    return manifest


def validate_protected_export(output_zip: Path) -> dict[str, object]:
    forbidden_markers = (
        b"<Me ",
        b" device=",
        b"<MetadataEntry",
        b"HKQuantityTypeIdentifierBodyMass",
        b"HKQuantityTypeIdentifierHeight",
        b"HKCategoryTypeIdentifierMenstrualFlow",
        b"HKCategoryTypeIdentifierAbdominalCramps",
        b"HKCategoryTypeIdentifierBloating",
        b"HKCategoryTypeIdentifierAcne",
        b"HKCategoryTypeIdentifierInfrequentMenstrualCycles",
    )
    with zipfile.ZipFile(output_zip, "r") as archive:
        if archive.testzip() is not None:
            raise ValueError("Protected ZIP failed CRC validation")
        names = archive.namelist()
        if names != ["apple_health_export/export.xml", "PRIVACY_MANIFEST.json"]:
            raise ValueError(f"Unexpected archive members: {names}")
        xml_bytes = archive.read("apple_health_export/export.xml")
        found = [marker.decode("utf-8") for marker in forbidden_markers if marker in xml_bytes]
        if found:
            raise ValueError(f"Forbidden markers remain in protected XML: {found}")
        ET.fromstring(xml_bytes)
    return {
        "file_name": output_zip.name,
        "size_bytes": output_zip.stat().st_size,
        "sha256": hashlib.sha256(output_zip.read_bytes()).hexdigest(),
        "validated": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_zip", type=Path)
    parser.add_argument("output_zip", type=Path)
    args = parser.parse_args()
    manifest = create_protected_export(args.input_zip, args.output_zip)
    validation = validate_protected_export(args.output_zip)
    print(json.dumps({**validation, **manifest}, indent=2))


if __name__ == "__main__":
    main()

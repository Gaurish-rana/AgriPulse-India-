"""
AgriPulse India — Power BI Template (.pbit) Packager
====================================================
Packages model.bim and report.json into a native Power BI Template (.pbit)
which opens directly in Microsoft Power BI Desktop.
"""

import os
import zipfile
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PBIT_PATH = os.path.join(BASE_DIR, "AgriPulse_India.pbit")

CONTENT_TYPES_XML = """<?xml version="1.0" encoding="utf-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="json" ContentType="" />
  <Override PartName="/Version" ContentType="" />
  <Override PartName="/DataModelSchema" ContentType="" />
  <Override PartName="/Report/Layout" ContentType="" />
  <Override PartName="/Settings" ContentType="" />
</Types>"""

SETTINGS_JSON = json.dumps({"version": "2.128.0.0", "customVisuals": []})

with open(os.path.join(BASE_DIR, "AgriPulse_India.Dataset", "model.bim"), "r", encoding="utf-8") as f:
    model_bim_content = f.read()

with open(os.path.join(BASE_DIR, "AgriPulse_India.Report", "report.json"), "r", encoding="utf-8") as f:
    report_json_content = f.read()

with zipfile.ZipFile(PBIT_PATH, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("[Content_Types].xml", CONTENT_TYPES_XML.encode("utf-8"))
    z.writestr("Version", "2.128.0.0".encode("utf-16le"))
    z.writestr("Settings", SETTINGS_JSON.encode("utf-16le"))
    # DataModelSchema is UTF-16LE in native PBIT
    z.writestr("DataModelSchema", model_bim_content.encode("utf-16le"))
    # Report/Layout is UTF-16LE in native PBIT
    z.writestr("Report/Layout", report_json_content.encode("utf-16le"))

print(f"Power BI Template created successfully at: {PBIT_PATH} ({os.path.getsize(PBIT_PATH):,} bytes)")

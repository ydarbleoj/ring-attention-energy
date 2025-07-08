#!/usr/bin/env python3
"""
Debug CAISO API XML Error Response

Look at the exact XML error content to understand what's wrong.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
import zipfile
import io
import xml.etree.ElementTree as ET

def debug_caiso_xml_error():
    """Debug CAISO XML error response in detail."""

    print("🔍 CAISO XML Error Debug")
    print("=" * 30)

    base_url = "http://oasis.caiso.com/oasisapi/SingleZip"

    # Test the PRC_LMP query that's failing
    params = {
        'queryname': 'PRC_LMP',
        'startdatetime': '20240101T08:00-0000',
        'enddatetime': '20240102T08:00-0000',
        'version': '1',
        'market_run_id': 'RTM',
        'node': 'TH_NP15_GEN-APND',
        'resultformat': '6'
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        print(f"Status: {response.status_code}")
        print(f"URL: {response.url}")

        if response.status_code == 200:
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))

            for file_name in zip_file.namelist():
                print(f"\n📄 File: {file_name}")

                with zip_file.open(file_name) as f:
                    content = f.read()

                    if file_name.endswith('.xml'):
                        try:
                            root = ET.fromstring(content)
                            print(f"XML Structure:")
                            _print_xml_structure(root, indent=2)

                            # Look for specific error information
                            for elem in root.iter():
                                if elem.text and elem.text.strip():
                                    print(f"  {elem.tag}: {elem.text.strip()}")

                        except ET.ParseError as e:
                            print(f"XML Parse Error: {e}")
                            print(f"Raw XML:\n{content.decode('utf-8')}")
                    else:
                        print(f"Non-XML content: {content[:500]}...")

    except Exception as e:
        print(f"Error: {e}")

def _print_xml_structure(elem, indent=0):
    """Print XML structure recursively."""
    prefix = "  " * indent
    if elem.text and elem.text.strip():
        print(f"{prefix}{elem.tag}: {elem.text.strip()}")
    else:
        print(f"{prefix}{elem.tag}")

    for child in elem:
        _print_xml_structure(child, indent + 1)

if __name__ == "__main__":
    debug_caiso_xml_error()

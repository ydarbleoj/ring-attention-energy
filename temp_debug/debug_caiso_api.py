#!/usr/bin/env python3
"""
Debug CAISO API Response

Inspect the actual response content from CAISO API to understand what's being returned.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
import zipfile
import io
import xml.etree.ElementTree as ET

def debug_caiso_api():
    """Debug CAISO API to see what's actually being returned."""

    print("🔍 CAISO API Response Debug")
    print("=" * 40)

    base_url = "http://oasis.caiso.com/oasisapi/SingleZip"

    # Try different query types to see which ones work
    test_queries = [
        {
            'name': 'Real-time Demand (SLD)',
            'params': {
                'queryname': 'SLD_FCST',  # System Load and Demand Forecast
                'startdatetime': '20240101T08:00-0000',
                'enddatetime': '20240102T08:00-0000',
                'version': '1',
                'resultformat': '6'  # CSV format
            }
        },
        {
            'name': 'Locational Marginal Price (PRC_LMP)',
            'params': {
                'queryname': 'PRC_LMP',  # What we're currently using
                'startdatetime': '20240101T08:00-0000',
                'enddatetime': '20240102T08:00-0000',
                'version': '1',
                'market_run_id': 'RTM',
                'node': 'TH_NP15_GEN-APND',
                'resultformat': '6'
            }
        },
        {
            'name': 'Actual System Demand (SLD_REN_FCST)',
            'params': {
                'queryname': 'SLD_REN_FCST',  # System Load including Renewables
                'startdatetime': '20240101T08:00-0000',
                'enddatetime': '20240102T08:00-0000',
                'version': '3',
                'resultformat': '6'
            }
        }
    ]

    for i, test in enumerate(test_queries, 1):
        print(f"\n🧪 Test {i}: {test['name']}")
        print("-" * 30)

        try:
            response = requests.get(base_url, params=test['params'], timeout=30)
            print(f"   Status: {response.status_code}")
            print(f"   Content-Type: {response.headers.get('content-type', 'N/A')}")
            print(f"   Content-Length: {len(response.content)} bytes")

            if response.status_code == 200:
                # Check if it's a ZIP file
                if response.content.startswith(b'PK'):
                    print("   ✅ Response is a ZIP file")

                    try:
                        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                        print(f"   📁 ZIP contents: {zip_file.namelist()}")

                        for file_name in zip_file.namelist():
                            print(f"   📄 File: {file_name}")

                            # Read first few lines to see what's inside
                            with zip_file.open(file_name) as f:
                                content = f.read()
                                if file_name.endswith('.xml'):
                                    try:
                                        root = ET.fromstring(content)
                                        print(f"      XML root: {root.tag}")
                                        if hasattr(root, 'text') and root.text:
                                            print(f"      XML content preview: {root.text[:200]}...")
                                        # Look for error messages
                                        for elem in root.iter():
                                            if 'error' in elem.tag.lower() or 'message' in elem.tag.lower():
                                                print(f"      🚨 Error element: {elem.tag} = {elem.text}")
                                    except ET.ParseError as e:
                                        print(f"      ❌ XML parse error: {e}")
                                        print(f"      Raw content: {content[:500]}...")
                                elif file_name.endswith('.csv'):
                                    print(f"      📊 CSV preview:")
                                    lines = content.decode('utf-8').split('\n')[:5]
                                    for line in lines:
                                        print(f"         {line}")
                                else:
                                    print(f"      Unknown file type, raw content: {content[:200]}...")

                    except zipfile.BadZipFile as e:
                        print(f"   ❌ Bad ZIP file: {e}")
                        print(f"   Raw content preview: {response.content[:200]}...")

                else:
                    print("   📄 Response is text/other format")
                    text_content = response.text[:500]
                    print(f"   Content preview: {text_content}...")

            else:
                print(f"   ❌ HTTP Error: {response.status_code}")
                print(f"   Error content: {response.text[:300]}...")

        except Exception as e:
            print(f"   💥 Request failed: {e}")

    print(f"\n📚 CAISO API Documentation:")
    print(f"   Base: http://www.caiso.com/oasisapi/")
    print(f"   Docs: http://www.caiso.com/Pages/documentsbygroup.aspx?GroupID=41B850C2-33A4-4B02-B87E-5F1FE0074A79")

if __name__ == "__main__":
    debug_caiso_api()

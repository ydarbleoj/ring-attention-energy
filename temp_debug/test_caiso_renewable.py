#!/usr/bin/env python3
"""
Test CAISO Renewable Generation Queries

Test different renewable generation query types to find working ones.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
import zipfile
import io
import xml.etree.ElementTree as ET

def test_renewable_queries():
    """Test different renewable generation query types."""

    print("🔍 CAISO Renewable Generation Query Test")
    print("=" * 45)

    base_url = "http://oasis.caiso.com/oasisapi/SingleZip"

    # Test different renewable generation query types
    test_queries = [
        {
            'name': 'Renewable Generation Forecast (SLD_REN_FCST)',
            'params': {
                'queryname': 'SLD_REN_FCST',
                'startdatetime': '20240101T08:00-0000',
                'enddatetime': '20240102T08:00-0000',
                'version': '3',
                'resultformat': '6'
            }
        },
        {
            'name': 'Renewable Generation Actual (SLD_REN_FCST v1)',
            'params': {
                'queryname': 'SLD_REN_FCST',
                'startdatetime': '20240101T08:00-0000',
                'enddatetime': '20240102T08:00-0000',
                'version': '1',
                'resultformat': '6'
            }
        },
        {
            'name': 'Fuel Source Mix (FUEL_MIX)',
            'params': {
                'queryname': 'FUEL_MIX',
                'startdatetime': '20240101T08:00-0000',
                'enddatetime': '20240102T08:00-0000',
                'version': '1',
                'resultformat': '6'
            }
        },
        {
            'name': 'Generation by Resource Type (GEN_BY_RESOURCE_TYPE)',
            'params': {
                'queryname': 'GEN_BY_RESOURCE_TYPE',
                'startdatetime': '20240101T08:00-0000',
                'enddatetime': '20240102T08:00-0000',
                'version': '1',
                'resultformat': '6'
            }
        }
    ]

    for i, test in enumerate(test_queries, 1):
        print(f"\n🧪 Test {i}: {test['name']}")
        print("-" * 40)

        try:
            response = requests.get(base_url, params=test['params'], timeout=30)
            print(f"   Status: {response.status_code}")
            print(f"   Content-Length: {len(response.content)} bytes")

            if response.status_code == 200:
                if response.content.startswith(b'PK'):
                    print("   ✅ Response is a ZIP file")

                    try:
                        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                        files = zip_file.namelist()
                        print(f"   📁 ZIP contents: {files}")

                        for file_name in files:
                            if file_name.endswith('.csv'):
                                print(f"   📊 CSV file found: {file_name}")
                                with zip_file.open(file_name) as f:
                                    content = f.read().decode('utf-8')
                                    lines = content.split('\n')[:5]
                                    print(f"   📄 CSV preview:")
                                    for line in lines:
                                        print(f"      {line}")
                                    print(f"   📈 Total lines: {len(content.split()) - 1}")
                                break
                            elif file_name.endswith('.xml'):
                                print(f"   ⚠️  XML file (possible error): {file_name}")
                                with zip_file.open(file_name) as f:
                                    xml_content = f.read()
                                    try:
                                        root = ET.fromstring(xml_content)
                                        for elem in root.iter():
                                            if 'ERR_CODE' in elem.tag and elem.text:
                                                print(f"      🚨 Error code: {elem.text}")
                                            elif 'ERR_DESC' in elem.tag and elem.text:
                                                print(f"      🚨 Error desc: {elem.text}")
                                    except ET.ParseError:
                                        print(f"      ❌ Could not parse XML")

                    except zipfile.BadZipFile:
                        print(f"   ❌ Bad ZIP file")

                else:
                    print("   📄 Response is not a ZIP file")

            else:
                print(f"   ❌ HTTP Error: {response.status_code}")

        except Exception as e:
            print(f"   💥 Request failed: {e}")

    print(f"\n📚 Next Steps:")
    print(f"   - Use working query types in CAISO client")
    print(f"   - Update generation method with correct parameters")

if __name__ == "__main__":
    test_renewable_queries()

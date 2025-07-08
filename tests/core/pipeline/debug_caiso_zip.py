"""
Debug CAISO ZIP file contents
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import zipfile
import io
from src.core.integrations.caiso.client import CAISOClient

def debug_caiso_zip():
    client = CAISOClient()

    params = {
        'queryname': 'SLD_RTO',  # System Load Demand
        'startdatetime': '20240101T00:00-0000',
        'enddatetime': '20240102T23:59-0000',
        'version': '1',
        'market_run_id': 'RTM',
        'resultformat': '6'
    }

    try:
        response_data = client._make_request(params)
        print(f"Response type: {type(response_data)}")
        print(f"Response length: {len(response_data)}")

        if isinstance(response_data, bytes):
            print("Got bytes - this is a ZIP file!")
            zip_file = zipfile.ZipFile(io.BytesIO(response_data))

            print(f"Files in ZIP: {zip_file.namelist()}")

            for file_name in zip_file.namelist():
                print(f"\nFile: {file_name}")
                print(f"  Extension: {file_name.split('.')[-1] if '.' in file_name else 'no extension'}")

                # Read first few lines
                with zip_file.open(file_name) as f:
                    content = f.read()
                    print(f"  Content length: {len(content)} bytes")
                    print(f"  Content type: {type(content)}")

                    # Try to decode as text
                    try:
                        text_content = content.decode('utf-8')
                        lines = text_content.split('\n')[:5]
                        print(f"  First 5 lines:")
                        for i, line in enumerate(lines):
                            print(f"    {i}: {line}")
                    except Exception as e:
                        print(f"  Could not decode as text: {e}")
                        print(f"  Raw bytes preview: {content[:100]}")
        else:
            print("Got text response")
            print(f"First 200 chars: {response_data[:200]}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_caiso_zip()

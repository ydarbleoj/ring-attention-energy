#!/usr/bin/env python3
"""
Analyze the VCR cassette files to understand the API response structures
"""

import yaml
import json
from pathlib import Path
from pprint import pprint

def analyze_cassette(cassette_path: Path, cassette_name: str):
    """Analyze a single VCR cassette file"""
    print(f"\n{'='*60}")
    print(f"📼 ANALYZING: {cassette_name}")
    print(f"{'='*60}")

    with open(cassette_path, 'r') as f:
        cassette_data = yaml.safe_load(f)

    # Get the first (and likely only) interaction
    interaction = cassette_data['interactions'][0]

    # Analyze the request
    request = interaction['request']
    print(f"\n🔗 REQUEST URL:")
    print(f"   {request['uri']}")

    # Parse the URL to show query parameters
    from urllib.parse import urlparse, parse_qs
    parsed_url = urlparse(request['uri'])
    query_params = parse_qs(parsed_url.query)

    print(f"\n📋 KEY QUERY PARAMETERS:")
    for key, value in query_params.items():
        if key in ['facets[respondent][]', 'facets[type][]', 'start', 'end', 'frequency']:
            print(f"   {key}: {value[0] if value else 'None'}")

    # Analyze the response
    response_body = json.loads(interaction['response']['body']['string'])

    print(f"\n📊 RESPONSE SUMMARY:")
    print(f"   Total records: {response_body['response']['total']}")
    print(f"   Date format: {response_body['response']['dateFormat']}")
    print(f"   Frequency: {response_body['response']['frequency']}")
    print(f"   Data array length: {len(response_body['response']['data'])}")

    # Analyze the first few data records
    print(f"\n🔍 SAMPLE DATA STRUCTURE:")
    sample_records = response_body['response']['data'][:3]
    for i, record in enumerate(sample_records):
        print(f"\n   Record {i+1}:")
        for key, value in record.items():
            print(f"     {key}: {value}")

    # Unique fields analysis
    all_fields = set()
    for record in response_body['response']['data']:
        all_fields.update(record.keys())

    print(f"\n📚 ALL UNIQUE FIELDS:")
    for field in sorted(all_fields):
        print(f"   • {field}")

    # Field value analysis for key fields
    if 'type' in all_fields:
        types = set(record.get('type') for record in response_body['response']['data'])
        print(f"\n🏷️  TYPES: {types}")

    if 'fueltype' in all_fields:
        fueltypes = set(record.get('fueltype') for record in response_body['response']['data'] if record.get('fueltype'))
        print(f"\n⚡ FUEL TYPES: {sorted(fueltypes)}")

    return response_body


def main():
    """Main analysis function"""
    cassette_dir = Path("tests/core/integrations/eia/cassettes")

    cassettes = [
        ("eia_oregon_demand.yaml", "Oregon Electricity Demand"),
        ("eia_oregon_renewable.yaml", "Oregon Renewable Generation"),
        ("eia_oregon_comprehensive.yaml", "Oregon Comprehensive Data")
    ]

    results = {}

    for filename, description in cassettes:
        cassette_path = cassette_dir / filename
        if cassette_path.exists():
            results[description] = analyze_cassette(cassette_path, description)
        else:
            print(f"❌ Missing cassette: {filename}")

    # Summary comparison
    print(f"\n{'='*60}")
    print(f"📈 COMPARISON SUMMARY")
    print(f"{'='*60}")

    for name, data in results.items():
        total_records = data['response']['total']
        sample_count = len(data['response']['data'])
        print(f"\n{name}:")
        print(f"   • Total records available: {total_records}")
        print(f"   • Records in sample: {sample_count}")

        # Show unique data types/patterns
        sample_data = data['response']['data']

        if sample_data:
            first_record = sample_data[0]

            if 'type' in first_record:
                print(f"   • Data type: {first_record['type']} ({first_record.get('type-name', 'N/A')})")

            if 'fueltype' in first_record:
                fueltypes = set(record.get('fueltype') for record in sample_data if record.get('fueltype'))
                print(f"   • Fuel types: {len(fueltypes)} different types")
                print(f"     {', '.join(sorted(fueltypes))}")


if __name__ == "__main__":
    main()

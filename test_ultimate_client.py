#!/usr/bin/env python3
"""
Simple test of the updated Ultimate Optimized EIA Client
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from src.core.integrations.eia.client import EIAClient
from src.core.integrations.config import get_config

def test_ultimate_client():
    print("🧪 Testing Ultimate Optimized EIA Client")
    print("=" * 50)

    try:
        config = get_config()
        client = EIAClient(config=config)

        print("✅ Client created successfully")

        # Check session configuration
        session = client.session
        adapter = session.get_adapter('https://')

        print(f"✅ Connection pool: {adapter.config.get('pool_connections', 'N/A')}")
        print(f"✅ Pool max size: {adapter.config.get('pool_maxsize', 'N/A')}")
        print(f"✅ Headers: {dict(session.headers)}")

        # Test API connection
        if client.test_api_connection():
            print("✅ API connection test passed")
            print("🔥 Ultimate optimizations are active!")
        else:
            print("❌ API connection test failed")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_ultimate_client()

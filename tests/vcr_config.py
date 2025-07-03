"""
VCR (Video Cassette Recorder) configuration for API testing

VCR records HTTP interactions and replays them during tests, allowing us to:
1. Test API integration without hitting real endpoints repeatedly
2. Ensure reproducible tests that don't depend on external services
3. Avoid API rate limits during development and CI
4. Test error conditions and edge cases

Configuration follows production-ready testing practices for ML systems.
"""
import vcr
import pytest
from pathlib import Path
from typing import Dict, Any


def scrub_sensitive_data(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scrub sensitive data from VCR cassettes

    This ensures API keys and other secrets don't get committed to git
    """
    # Scrub API keys from URLs and headers
    if 'url' in response:
        # Replace API keys in URLs
        url = response['url']
        if 'api_key=' in url:
            url = url.split('api_key=')[0] + 'api_key=REDACTED'
            response['url'] = url

    # Scrub headers that might contain sensitive info
    if 'headers' in response:
        headers = response['headers']
        sensitive_headers = ['authorization', 'x-api-key', 'api-key']
        for header in sensitive_headers:
            if header in headers:
                headers[header] = ['REDACTED']

    return response


def create_vcr_config(cassette_name: str, cassette_dir: Path = None) -> vcr.VCR:
    """
    Create a VCR instance with our standard configuration

    Args:
        cassette_name: Name of the cassette file (without extension)
        cassette_dir: Directory to store cassettes (defaults to tests/vcr_cassettes)

    Returns:
        Configured VCR instance
    """
    if cassette_dir is None:
        cassette_dir = Path("tests/vcr_cassettes")

    cassette_dir.mkdir(parents=True, exist_ok=True)
    cassette_path = cassette_dir / f"{cassette_name}.yaml"

    return vcr.VCR(
        # Store cassettes in YAML format for readability
        serializer='yaml',

        # Cassette file location
        cassette_library_dir=str(cassette_dir),

        # Match requests by method, URI, and body
        match_on=['method', 'uri', 'body'],

        # Record new interactions, replay existing ones
        record_mode='once',

        # Scrub sensitive data
        before_record_response=scrub_sensitive_data,

        # Filter out sensitive query parameters
        filter_query_parameters=['api_key', 'key', 'token'],

        # Filter sensitive headers
        filter_headers=['authorization', 'x-api-key', 'api-key'],

        # Decode compressed responses for readability
        decode_compressed_response=True,
    )


# Pytest fixtures for common VCR scenarios
@pytest.fixture
def vcr_config():
    """Basic VCR configuration for tests"""
    return {
        'serializer': 'yaml',
        'match_on': ['method', 'uri', 'body'],
        'record_mode': 'once',
        'filter_query_parameters': ['api_key', 'key', 'token'],
        'filter_headers': ['authorization', 'x-api-key', 'api-key'],
    }


@pytest.fixture
def eia_vcr():
    """VCR instance configured for EIA API testing"""
    return create_vcr_config("eia_api_test")


@pytest.fixture
def caiso_vcr():
    """VCR instance configured for CAISO API testing"""
    return create_vcr_config("caiso_api_test")


@pytest.fixture
def entso_e_vcr():
    """VCR instance configured for ENTSO-E API testing"""
    return create_vcr_config("entso_e_api_test")


# Context manager for easy VCR usage
class VCRManager:
    """
    Context manager for VCR cassette recording/playback

    Usage:
        with VCRManager("test_cassette") as vcr_manager:
            # Make API calls here
            response = requests.get("https://api.example.com/data")
    """

    def __init__(self, cassette_name: str, cassette_dir: Path = None):
        self.cassette_name = cassette_name
        self.cassette_dir = cassette_dir or Path("tests/vcr_cassettes")
        self.vcr_instance = None
        self.cassette = None

    def __enter__(self):
        self.vcr_instance = create_vcr_config(self.cassette_name, self.cassette_dir)
        self.cassette = self.vcr_instance.use_cassette(f"{self.cassette_name}.yaml")
        self.cassette.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cassette:
            self.cassette.__exit__(exc_type, exc_val, exc_tb)


# Helper function for test data validation
def validate_energy_data_response(data: Dict[str, Any]) -> bool:
    """
    Validate that API response contains expected energy data structure

    Args:
        data: API response data

    Returns:
        True if data structure is valid
    """
    # Basic structure validation
    if not isinstance(data, dict):
        return False

    # Check for common energy data fields
    expected_fields = ['response', 'data']
    if not any(field in data for field in expected_fields):
        return False

    return True


# Example usage and testing
if __name__ == "__main__":
    # Demo VCR setup
    print("VCR configuration created successfully!")

    # Test cassette directory creation
    test_dir = Path("tests/vcr_cassettes")
    vcr_instance = create_vcr_config("demo_test", test_dir)

    print(f"Cassette directory: {test_dir}")
    print(f"VCR instance: {vcr_instance}")
    print("Ready for API testing!")

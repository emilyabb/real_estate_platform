"""Shared utilities for HUD API ingestion.

This module provides common functionality used by both reference data ingestion
(states, counties) and recurring Fair Market Rent (FMR) data ingestion.
"""

import requests
import json
from http import HTTPStatus
from datetime import datetime
from typing import Optional, Dict, Any


# ============================================================================
# Configuration
# ============================================================================

BASE_URL = "https://www.huduser.gov/hudapi/public"
BRONZE_CATALOG = "bronze_dev"
BRONZE_SCHEMA = "hud"
VOLUME_BASE_PATH = "/Volumes/bronze_dev/hud/hud_raw"


# ============================================================================
# API Client
# ============================================================================

def get_hud_token(dbutils):
    """Retrieve HUD API token from Databricks secrets.
    
    Args:
        dbutils: Databricks utilities object
        
    Returns:
        str: HUD API token
    """
    token = dbutils.secrets.get(
        scope="api-secrets",
        key="hud-token"
    ).lstrip("\x00")
    return token


def retrieve_hud_json(url: str, token: str) -> Optional[Dict[str, Any]]:
    """Make a GET request to the HUD API and return JSON response.
    
    Args:
        url: Full API endpoint URL
        token: HUD API authentication token
        
    Returns:
        Dict with 'url' and 'data' keys, or None if request fails
    """
    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        status_desc = HTTPStatus(response.status_code).phrase
        print(f"Request failed with status code {response.status_code} \"{status_desc}\" for {url}")
        return None
    else:
        print(f"Successful for {url}")
    
    data = response.json()
    
    return {'url': url, 'data': data}


# ============================================================================
# Data Persistence
# ============================================================================

def save_to_volume(data: Any, filename: str, volume_path: str = VOLUME_BASE_PATH) -> str:
    """Save data as JSON to a Unity Catalog volume.
    
    Args:
        data: Data to save (must be JSON serializable)
        filename: Name of the file (e.g., 'counties_20260818.json')
        volume_path: Base volume path (defaults to VOLUME_BASE_PATH)
        
    Returns:
        str: Full path where the file was saved
    """
    full_path = f"{volume_path}/{filename}"
    
    with open(full_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    
    print(f"Saved to {full_path}")
    return full_path


def generate_timestamped_filename(prefix: str, extension: str = "json") -> str:
    """Generate a filename with timestamp.
    
    Args:
        prefix: Filename prefix (e.g., 'counties', 'fmr')
        extension: File extension without dot (default: 'json')
        
    Returns:
        str: Filename like 'counties_20260818_123045.json'
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}.{extension}"


# ============================================================================
# URL Builders
# ============================================================================

def build_list_states_url() -> str:
    """Build URL to list all states."""
    return f"{BASE_URL}/fmr/listStates"


def build_list_counties_url(state_code: str) -> str:
    """Build URL to list counties for a given state.
    
    Args:
        state_code: Two-letter state code (e.g., 'AL', 'CA')
        
    Returns:
        str: URL to fetch counties for the state
    """
    url = f"{BASE_URL}/fmr/listCounties/{state_code}"
    # Special case for Connecticut
    if state_code == "CT":
        url += "?updated=2025"
    return url


def build_fmr_data_url(fips_code: str) -> str:
    """Build URL to fetch Fair Market Rent data for a county.
    
    Args:
        fips_code: County FIPS code (e.g., '0100199999')
        
    Returns:
        str: URL to fetch FMR data for the county
    """
    return f"{BASE_URL}/fmr/data/{fips_code}"

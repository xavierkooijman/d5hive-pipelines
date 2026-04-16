from flask import logging
import requests


def fetch_data_from_api(url, params=None, headers=None, timeout=10):
    try:
        response = requests.get(
            url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        raise

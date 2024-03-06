"""
Take a csv of unique identifiers and filenames from Cortex and convert those values into the desired format for Newberry Transcribe Omeka S ingest.
Requires a config.py file with Cortex API token.
"""

import pandas as pd
import argparse
import config
import requests
import re


def authenticate_cortex():
    authenticate_url = f'https://collections.newberry.org/api/Authentication/v2.0/Login?Login={config.username}&Password={config.password}&IncludeUserEmail=false&format=json&api_key=authenticate'
    authenticate = requests.post(authenticate_url)
    token = authenticate.json()['APIResponse']['Response']['Token']
    return f'&token={token}', '&format=json'


def fetch_iiif_values(df, token, json_suffix):
    iiif_column = 'IIIF Value'
    df[iiif_column] = None

    for value in df['Unique identifier']:
        api_url = f'https://collections.newberry.org/API/search/v3.0/search?query=SystemIdentifier:{value}&fields=MediaEncryptedIdentifier{token}{json_suffix}'
        response = requests.get(api_url)

        if response.status_code == 200:
            data = response.json()
            if "APIResponse" in data and "Items" in data["APIResponse"] and data["APIResponse"]["Items"]:
                media_encrypted_identifier = data['APIResponse']['Items'][0]['MediaEncryptedIdentifier']
                df.loc[df['Unique identifier'] == value, iiif_column] = f'https://collections.newberry.org/IIIF3/Image/{media_encrypted_identifier}/info.json'


def extract_title(df):
    title_column = 'Title'
    df[title_column] = None

    for value in df['Original file name']:
        pattern = r".*fl_(\d+)_?(\d+)_?(\d+)?\.tif"

        # Extract the relevant parts using regular expressions
        match = re.match(pattern, value)

        if match:
            # Extract the section and item numbers
            folder_number = match.group(1).lstrip("0")
            section_number = match.group(2).lstrip("0")  # Remove leading zeros
            page_number = match.group(3) or 0

            # Format the result
            if page_number == 0:
                result = f'Page {folder_number}.{section_number}'
            else:
                result = f'Page {folder_number}.{section_number}.{page_number.lstrip("0")}'

            df.loc[df['Original file name'] == value, title_column] = result
        else:
            print(f'Filename format does not match for {value}.')


def update_csv(df, csv_filename):
    df.to_csv(csv_filename, index=False)
    print('Your csv has been updated :)')

if __name__ == "__main__":
    # Select csv input
    parser = argparse.ArgumentParser()
    parser.add_argument('your_value', nargs='?', help='csv must have columns of cortex unique identifiers and filenames.')
    args = parser.parse_args()

    # Authenticate session in Cortex
    token, json_suffix = authenticate_cortex()

    # Load the DataFrame from CSV file
    df = pd.read_csv(args.your_value)

    # Fetch IIIF values
    fetch_iiif_values(df, token, json_suffix)

    # Extract titles
    extract_title(df)

    # Write the updated DataFrame back to the CSV file
    update_csv(df, args.your_value)

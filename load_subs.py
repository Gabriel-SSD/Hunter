import os.path
import csv
from logger import setup_logging
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set up the logger
logger = setup_logging()

# Define the API scopes and the spreadsheet info
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SAMPLE_SPREADSHEET_ID = "1uuL0tgLyJzLjp9nefNSYx54mvHpI4AkCjS-vafsG0ec"
SAMPLE_RANGE_NAME = "Subs!A1:C"  # Read from row 1 to the last row of column C


def main():
    """Fetch data from Google Sheets and save it to a CSV file, with logging for progress and errors."""
    creds = None
    # Check if token.json exists and load credentials
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        logger.info("Loaded credentials from token.json.")

    # If credentials are invalid or missing, initiate login flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            logger.info("Credentials expired, refreshing token.")
        else:
            logger.info("No valid credentials found, initiating login flow.")
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for future use
        with open("token.json", "w") as token:
            token.write(creds.to_json())
        logger.info("Credentials saved to token.json.")

    try:
        # Build the Sheets API service
        service = build("sheets", "v4", credentials=creds)
        logger.info("Sheets API service built successfully.")

        sheet = service.spreadsheets()
        # Fetch the data from the specified range
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME).execute()
        values = result.get("values", [])

        # Check if any data was returned
        if not values:
            logger.warning("No data found in the specified range.")
            return

        # Log the fetched data for validation
        logger.info(f"Fetched {len(values)} rows of data from the spreadsheet.")

        # Define the output CSV filename
        csv_filename = "subs_data.csv"
        with open(csv_filename, mode="w", newline="") as file:
            writer = csv.writer(file)

            # Write the header (first row) into the CSV
            writer.writerow(values[0])
            logger.info("Header written to the CSV file.")

            # Write the remaining rows of data into the CSV
            writer.writerows(values[1:])
            logger.info(f"Data written to {csv_filename}.")

        # Log success message
        logger.info(f"File {csv_filename} has been successfully generated.")

    except HttpError as err:
        # Log any errors that occur while accessing the Google Sheets API
        logger.error(f"An error occurred while accessing the Google Sheets API: {err}")


if __name__ == "__main__":
    main()

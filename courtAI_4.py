import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import googleapiclient.errors
from datetime import datetime, timezone, timedelta

# Load your service account credentials
SERVICE_ACCOUNT_FILE = '/Users/sampage/courtAI/data/automated-search-1720136852760-39ff654c2549.json'
SCOPES = ['https://www.googleapis.com/auth/calendar']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Initialize the Calendar API
service = build('calendar', 'v3', credentials=credentials)

# Calendar ID
calendar_id = 'c_13b6a2e4d923d02fbd765088c23a155ed60e0d12dc1e026ae99a70cf54b112ea@group.calendar.google.com'

def get_tomorrow_midnight_utc():
    """Returns tomorrow's date at midnight in UTC format for API filtering."""
    tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
    tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    return tomorrow.isoformat()

def list_tomorrow_and_future_events(service, calendar_id):
    """Retrieve only events from tomorrow onwards (excluding today and past events)."""
    try:
        events = []
        tomorrow_midnight_utc = get_tomorrow_midnight_utc()
        page_token = None
        while True:
            events_result = service.events().list(
                calendarId=calendar_id,
                timeMin=tomorrow_midnight_utc,  # Fetch only events from tomorrow onwards
                singleEvents=True,
                orderBy='startTime',
                pageToken=page_token
            ).execute()

            events.extend(events_result.get('items', []))
            page_token = events_result.get('nextPageToken')
            if not page_token:
                break

        return events
    except googleapiclient.errors.HttpError as error:
        print(f"An error occurred while listing events: {error}")
        return []

def batch_delete_callback(request_id, response, exception):
    """Callback function for batch deletion."""
    if exception is not None:
        print(f"Error deleting event: {exception}")
    else:
        print(f"Successfully deleted event: {request_id}")

def clear_tomorrow_and_future_events(service, calendar_id):
    """Clears only events from tomorrow onwards using batch processing."""
    events = list_tomorrow_and_future_events(service, calendar_id)
    if not events:
        print("No events found from tomorrow onwards.")
        return

    batch = service.new_batch_http_request(callback=batch_delete_callback)

    for event in events:
        batch.add(service.events().delete(calendarId=calendar_id, eventId=event['id']))
    
    print(f"Deleting {len(events)} events from tomorrow onwards in batch...")
    batch.execute()
    print("All events from tomorrow onwards have been cleared.")

# List tomorrow's and future events first
tomorrow_and_future_events = list_tomorrow_and_future_events(service, calendar_id)
if tomorrow_and_future_events:
    print(f"Found {len(tomorrow_and_future_events)} events from tomorrow onwards:")
    for event in tomorrow_and_future_events:
        print(f" - {event.get('summary', 'No Title')} (ID: {event['id']})")

    # Clear only events from tomorrow onwards
    clear_tomorrow_and_future_events(service, calendar_id)
else:
    print("No events found from tomorrow onwards in the calendar.")

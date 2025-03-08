import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import googleapiclient.errors
from datetime import datetime, timezone, timedelta
import time  # For API rate-limiting handling

# Load your service account credentials
SERVICE_ACCOUNT_FILE = '/Users/sampage/courtAI/data/automated-search-1720136852760-39ff654c2549.json'
SCOPES = ['https://www.googleapis.com/auth/calendar']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Initialize the Calendar API
service = build('calendar', 'v3', credentials=credentials)

# Calendar ID
calendar_id = 'c_13b6a2e4d923d02fbd765088c23a155ed60e0d12dc1e026ae99a70cf54b112ea@group.calendar.google.com'

# Read the CSV file
df = pd.read_csv('/Users/sampage/courtAI/Results/search_results.csv')

# Check if 'Location' column exists
has_location = 'Location' in df.columns

# Group entries by name and date, then aggregate links
def format_links(links):
    return '\n'.join([f'{url.split("/")[-1]}: {url}' for url in links])

grouping_columns = ['Name', 'Date']
aggregation_functions = {
    'Case Number': 'first',
    'Hearing Type': 'first',
    'Title': 'first',
    'Link': lambda x: format_links(x),
    'Time': 'first'
}

if has_location:
    aggregation_functions['Location'] = 'first'

grouped = df.groupby(grouping_columns).agg(aggregation_functions).reset_index()

def get_color_id(hearing_type):
    """Assign a color based on hearing type."""
    color_map = {
        'Motions Hearing': '9',  # Blue
        'First Appearance': '10',  # Green
        'Competency to Proceed Hearing': '3',  # Purple
        'Plea Hearing': '11',  # Red
        'Preliminary Hearing': '5',  # Yellow
        'Status Conference': '8',  # Black
    }
    return color_map.get(hearing_type, '11')  # Default color

def fetch_existing_events(service, calendar_id, start_time, end_time):
    """Fetch events within a given time range to avoid duplicates."""
    events_result = service.events().list(
        calendarId=calendar_id,
        timeMin=start_time,
        timeMax=end_time,
        singleEvents=True
    ).execute()
    
    existing_events = events_result.get('items', [])
    return {event['summary'] for event in existing_events}  # Return event titles for deduplication

def create_events_in_batch(service, calendar_id, events):
    """Creates events in batch to optimize performance."""
    batch = service.new_batch_http_request()
    
    for event in events:
        batch.add(service.events().insert(calendarId=calendar_id, body=event))
    
    print(f"Inserting {len(events)} events in batch...")
    batch.execute()
    print("Batch insert complete.")

# Prepare events for batch insert
batch_size = 10  # Process events in chunks of 10
event_batch = []
existing_event_titles = set()  # Cache to store existing events

for index, row in grouped.iterrows():
    try:
        start_time_str = f"{row['Date']} {row['Time']}"

        # Fix time formatting issue (e.g., "0:30 PM" to "12:30 PM")
        if row['Time'].startswith('0:'):
            start_time_str = start_time_str.replace('0:', '12:')

        start_time = datetime.strptime(start_time_str, '%Y-%m-%d %I:%M %p')
        end_time = start_time + timedelta(hours=1)  # Default 1-hour event

        start_time_iso = start_time.isoformat() + "Z"
        end_time_iso = end_time.isoformat() + "Z"

        # Fetch existing events only once per day
        if row['Date'] not in existing_event_titles:
            existing_event_titles = fetch_existing_events(service, calendar_id, start_time_iso, end_time_iso)

        event_title = f"{row['Name']} - {row['Hearing Type']}"
        
        # Skip duplicate events
        if event_title in existing_event_titles:
            print(f"Skipping duplicate event: {event_title}")
            continue

        event = {
            'summary': event_title,
            'description': f"Case Number: {row['Case Number']}\nTitle: {row['Title']}\nLinks:\n{row['Link']}",
            'start': {
                'dateTime': start_time_iso,
                'timeZone': 'America/Denver',  
            },
            'end': {
                'dateTime': end_time_iso,
                'timeZone': 'America/Denver',  
            },
            'colorId': get_color_id(row['Hearing Type'])
        }

        if has_location:
            event['location'] = row['Location']

        event_batch.append(event)

        # Process in batches to avoid API rate limits
        if len(event_batch) >= batch_size:
            create_events_in_batch(service, calendar_id, event_batch)
            event_batch = []  # Reset batch
        
        time.sleep(0.1)  # Prevent hitting API rate limits

    except Exception as e:
        print(f"Error processing event {row['Name']}: {e}")

# Process remaining events
if event_batch:
    create_events_in_batch(service, calendar_id, event_batch)

print("All events have been processed successfully!")

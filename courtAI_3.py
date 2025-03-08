import os
import logging
import requests
import time
import pandas as pd
import re
import json
from bs4 import BeautifulSoup

# Import tqdm with fallback
try:
    from tqdm import tqdm
except ImportError:
    # Simple fallback if tqdm is not available
    def tqdm(iterable, **kwargs):
        print(kwargs.get('desc', ''))
        return iterable

# 🔹 Load configuration from config.json
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config.json: {e}")
        print("Using default configuration")
        # Return default configuration
        return {
            "api_key": "",
            "cse_id": "",
            "results_folder": "~/courtAI/Results/",
            "input_file": "~/courtAI/Dockets/docket_codeready.csv",
            "request_timeout": 20,
            "search_delay": 3,
            "excluded_authors": ["associated press", "cnn newsource", "cnn", "debra worley", "the associated press", "gray news"],
            "excluded_url_patterns": ["entertainment/", "sports/", "/lifestyle/"],
            "year_matching": True  # Added default configuration for year matching
        }

# Load config
CONFIG = load_config()

# 🔹 File Paths
RESULTS_FOLDER = os.path.expanduser(CONFIG.get("results_folder", "~/courtAI/Results/"))
INPUT_FILE = os.path.expanduser(CONFIG.get("input_file", "~/courtAI/Dockets/docket_codeready.csv"))
LOG_FILE = os.path.join(RESULTS_FOLDER, "search_log.txt")
RESULTS_FILE = os.path.join(RESULTS_FOLDER, "search_results.csv")
EXCLUDED_RESULTS_FILE = os.path.join(RESULTS_FOLDER, "excluded_results.csv")
NO_RESULTS_FILE = os.path.join(RESULTS_FOLDER, "no_results.txt")
PROCESSED_NAMES_FILE = os.path.join(RESULTS_FOLDER, "processed_names.txt")

# 🔹 API Config (from config.json)
API_KEY = CONFIG.get("api_key", "")
CSE_ID = CONFIG.get("cse_id", "")
REQUEST_TIMEOUT = CONFIG.get("request_timeout", 20)
SEARCH_DELAY = CONFIG.get("search_delay", 3)

# 🔹 Filters
EXCLUDED_AUTHORS = set(CONFIG.get("excluded_authors", ["associated press", "cnn newsource", "cnn", "debra worley", "the associated press", "gray news"]))
EXCLUDED_URL_PATTERNS = CONFIG.get("excluded_url_patterns", [])
YEAR_MATCHING = CONFIG.get("year_matching", True)  # Control year matching filter

# 🔹 Setup Logging
def setup_logging():
    # Ensure the directory exists
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info("🚀 Script started")
    
    # Validate API credentials
    if not API_KEY or not CSE_ID:
        logging.error("❌ API_KEY or CSE_ID is missing in config.json")
        print("ERROR: API_KEY or CSE_ID is missing in config.json")
        return False
    return True

# 🔹 Clear Old Files Before Running
def clear_old_files():
    # First, make sure the result folder exists
    os.makedirs(RESULTS_FOLDER, exist_ok=True)
    
    # Close any open logging handlers before removing the log file
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)
    
    print("🗑️ Clearing all previous files...")
    
    # Delete the log file first
    if os.path.exists(LOG_FILE):
        try:
            os.remove(LOG_FILE)
            print(f"🗑️ Deleted old log file: {LOG_FILE}")
        except Exception as e:
            print(f"Error deleting log file: {e}")
    
    # Delete all other result files
    for file in [RESULTS_FILE, EXCLUDED_RESULTS_FILE, NO_RESULTS_FILE, PROCESSED_NAMES_FILE]:
        if os.path.exists(file):
            try:
                os.remove(file)
                print(f"🗑️ Deleted old file: {file}")
            except Exception as e:
                print(f"Error deleting file {file}: {e}")
    
    # Now reinitialize logging after clearing files
    setup_logging()
    logging.info("🗑️ All previous files cleared")

# 🔹 Read Input CSV
def read_names_from_csv():
    try:
        df = pd.read_csv(INPUT_FILE, dtype=str)
        names = df.values.tolist()
        logging.info(f"📋 Read {len(names)} names from input file")
        return names
    except Exception as e:
        logging.error(f"❌ Error reading input CSV: {e}")
        return []

# 🔹 Load Processed Names
def load_processed_names():
    processed = {}
    if os.path.exists(PROCESSED_NAMES_FILE):
        with open(PROCESSED_NAMES_FILE, "r") as f:
            for line in f:
                processed[line.strip()] = True
        logging.info(f"📋 Loaded {len(processed)} previously processed names")
    return processed

# 🔹 Save Processed Name
def save_processed_name(name):
    os.makedirs(os.path.dirname(PROCESSED_NAMES_FILE), exist_ok=True)
    with open(PROCESSED_NAMES_FILE, "a") as f:
        f.write(f"{name}\n")

# 🔹 Check if URL matches any excluded pattern
def is_excluded_url(url):
    for pattern in EXCLUDED_URL_PATTERNS:
        if pattern in url:
            return True
    return False

# 🔹 Extract Year from Case Number
def extract_year_from_case_number(case_number):
    # Common formats: YY-XXXX, YYYY-XXXX, YY-XX-XXXX, etc.
    # Look for patterns starting with 2 or 4 digit year
    if not case_number:
        return None
        
    # Try to match 2-digit or 4-digit year patterns
    match = re.search(r'^(\d{2}|\d{4})[-\s]', str(case_number))
    if match:
        year_str = match.group(1)
        if len(year_str) == 2:
            # Convert 2-digit year to 4-digit (assuming 20XX)
            return int("20" + year_str)
        return int(year_str)
        
    # Try to find any 4-digit number that could be a year (between 1980 and current year)
    match = re.search(r'(19[8-9]\d|20[0-2]\d)', str(case_number))
    if match:
        return int(match.group(1))
        
    return None

# 🔹 Improved Google Search Function - Properly handles "no results"
def perform_google_search(query, retries=3):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "q": f'{query} ',
        "key": API_KEY,
        "cx": CSE_ID,
        "num": 10  # Fetch more results
    }

    for attempt in range(retries):
        try:
            logging.info(f"🔍 Attempt {attempt+1} for query: {query}")
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            # If we get a valid response, return it regardless of whether it has results
            if "searchInformation" in data:
                # Log if no results were found
                if "items" not in data:
                    logging.info(f"ℹ️ No search results found for {query}")
                else:
                    logging.info(f"✅ Found {len(data['items'])} results for {query}")
                return data
                
            # Only retry for actual errors, not for "no results" cases
            logging.warning(f"⚠️ Invalid response format for {query}, retrying in {2**attempt} seconds...")
            time.sleep(2**attempt)  # Exponential backoff
        except requests.exceptions.RequestException as err:
            logging.error(f"❌ Error fetching results for {query}: {err}")
            if attempt < retries - 1:  # Only sleep if we're going to retry
                time.sleep(2**attempt)

    logging.error(f"❌ All retries failed for query: {query}")
    return None  # Return None after all retries

# 🔹 Extract Year from URL
def extract_year_from_url(url):
    match = re.search(r"/(\d{4})/", url)
    return int(match.group(1)) if match else None

# 🔹 Fetch Article Author
def fetch_author(article_url):
    try:
        response = requests.get(article_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            logging.warning(f"⚠️ Got status code {response.status_code} for {article_url}")
            return "No author"
            
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Expanded selector for more site support
        author_tag = soup.select_one("span.author, div.meta__user.vcard.author > a, .byline-name, .author-name, [rel='author'], .article-byline, .story-meta .name")
        
        if author_tag:
            author = author_tag.get_text(strip=True)
            logging.info(f"👤 Found author: {author} for {article_url}")
            return author
        
        # Try meta tags if direct tags failed
        meta_author = soup.select_one('meta[name="author"]')
        if meta_author and meta_author.get('content'):
            return meta_author.get('content')
            
        return "Unknown"
    except Exception as e:
        logging.error(f"❌ Error fetching author from {article_url}: {e}")
        return "Unknown"

# 🔹 Process Search Results with Year Matching
def process_search_results(search_results, case_number=None):
    if not search_results or "items" not in search_results:
        return [], []

    valid_results, excluded_results = [], []
    case_year = extract_year_from_case_number(case_number) if case_number else None
    
    if case_year:
        logging.info(f"🔢 Extracted case year: {case_year} from case number: {case_number}")
    else:
        logging.info(f"⚠️ Could not extract year from case number: {case_number}")
    
    for item in search_results.get("items", []):
        title = item.get("title", "")
        link = item.get("link", "")
        snippet = item.get("snippet", "")
        
        article_year = extract_year_from_url(link)
        logging.info(f"📰 Processing article: {title[:30]}... from {article_year or 'unknown year'}")
        
        author = fetch_author(link)
        
        # Determine exclusion reason
        exclusion_reason = None
        
        # Check if author is excluded
        if author.lower() in EXCLUDED_AUTHORS:
            exclusion_reason = f"Excluded author: {author}"
            logging.info(f"⏭️ {exclusion_reason}")
        
        # Check if URL matches any excluded patterns
        elif is_excluded_url(link):
            exclusion_reason = f"URL pattern match: {[p for p in EXCLUDED_URL_PATTERNS if p in link][0]}"
            logging.info(f"⏭️ {exclusion_reason}")
        
        # Check for year mismatch
        elif YEAR_MATCHING and case_year and article_year and case_year != article_year:
            exclusion_reason = f"Year mismatch: Case year {case_year} ≠ Article year {article_year}"
            logging.info(f"⏭️ {exclusion_reason}")
        
        # Add to appropriate results list
        if exclusion_reason:
            excluded_results.append((title, link, article_year, snippet, author, exclusion_reason))
        else:
            valid_results.append((title, link, article_year, snippet, author))

    logging.info(f"✅ Processed {len(valid_results)} valid, {len(excluded_results)} excluded results")
    return valid_results, excluded_results

# 🔹 Save Valid Results
def save_valid_results(name_info, results, file_path):
    if not results:
        return
    
    try:
        # Ensure all name_info elements are strings
        name_info_str = [str(item) if item is not None else "" for item in name_info]
        
        df_new = pd.DataFrame([(name_info_str + list(r)) for r in results], columns=[
            "Date", "Time", "Name", "Case Number", "Hearing Type", "Location",
            "Title", "Link", "Year", "Snippet", "Author"
        ])

        if os.path.exists(file_path):
            df_existing = pd.read_csv(file_path, dtype=str)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            # Remove duplicates but keep the first occurrence
            df_combined = df_combined.drop_duplicates(subset=["Name", "Link"], keep='first')
        else:
            df_combined = df_new

        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df_combined.to_csv(file_path, index=False)
        logging.info(f"✅ Saved {len(df_new)} results to {file_path}")
    except Exception as e:
        logging.error(f"❌ Error saving results to {file_path}: {e}")

# 🔹 Save Excluded Results with Reason
def save_excluded_results(name_info, results, file_path):
    if not results:
        return
    
    try:
        # Ensure all name_info elements are strings
        name_info_str = [str(item) if item is not None else "" for item in name_info]
        
        df_new = pd.DataFrame([(name_info_str + list(r)) for r in results], columns=[
            "Date", "Time", "Name", "Case Number", "Hearing Type", "Location",
            "Title", "Link", "Year", "Snippet", "Author", "Exclusion Reason"
        ])

        if os.path.exists(file_path):
            df_existing = pd.read_csv(file_path, dtype=str)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            # Remove duplicates but keep the first occurrence
            df_combined = df_combined.drop_duplicates(subset=["Name", "Link"], keep='first')
        else:
            df_combined = df_new

        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df_combined.to_csv(file_path, index=False)
        logging.info(f"✅ Saved {len(df_new)} excluded results to {file_path}")
    except Exception as e:
        logging.error(f"❌ Error saving excluded results to {file_path}: {e}")

# 🔹 Improved Fetch & Process Name
def fetch_and_process_name(name_info, processed_names):
    try:
        # Extract name field with better error handling
        if len(name_info) >= 3:
            name = str(name_info[2])  # Convert to string to be safe
        else:
            logging.error(f"❌ Invalid name_info format (insufficient fields): {name_info}")
            return
            
        if not name or name.strip() == "":
            logging.warning(f"⚠️ Empty name found in data: {name_info}")
            return
            
        if name in processed_names:
            logging.info(f"⏭️ Skipping already processed name: {name}")
            return
        
        # Extract case number (index 3 if available)
        case_number = str(name_info[3]) if len(name_info) > 3 and name_info[3] is not None else None
        logging.info(f"🔍 Searching for: {name} (Case number: {case_number})")
        
        search_results = perform_google_search(f'"{name}"')

        # Even if search_results is valid but contains no items, we properly record it as "no results"
        if not search_results or "items" not in search_results:
            logging.info(f"📝 Recording no results for {name}")
            os.makedirs(os.path.dirname(NO_RESULTS_FILE), exist_ok=True)
            with open(NO_RESULTS_FILE, "a") as f:
                f.write(f"{name}\n")
            save_processed_name(name)  # Mark as processed
            return
        
        valid_results, excluded_results = process_search_results(search_results, case_number)

        if valid_results:
            save_valid_results(name_info, valid_results, RESULTS_FILE)
        if excluded_results:
            save_excluded_results(name_info, excluded_results, EXCLUDED_RESULTS_FILE)

        # Mark as processed
        save_processed_name(name)
        processed_names[name] = True
        
    except Exception as e:
        logging.error(f"❌ Error processing name: {e}")

# 🔹 Main Execution
def main():
    # Create necessary directories first
    os.makedirs(RESULTS_FOLDER, exist_ok=True)
    
    # Clear all previous files (including the log file) at the start
    clear_old_files()
    
    # Validate config after setup_logging has been called in clear_old_files
    if not API_KEY or not CSE_ID:
        logging.error("❌ API_KEY or CSE_ID is missing in config.json")
        print("ERROR: API_KEY or CSE_ID is missing in config.json")
        return
    
    # Log filtering settings
    logging.info(f"⚙️ URL filtering enabled with {len(EXCLUDED_URL_PATTERNS)} patterns: {EXCLUDED_URL_PATTERNS}")
    logging.info(f"⚙️ Year matching filter {'enabled' if YEAR_MATCHING else 'disabled'}")
    
    names = read_names_from_csv()
    processed_names = load_processed_names()  # This will be empty now since we cleared it
    
    # All names should be processed since processed_names is now empty
    remaining_names = []
    for name_info in names:
        if len(name_info) >= 3 and str(name_info[2]) not in processed_names:
            remaining_names.append(name_info)
    
    logging.info(f"🔄 Processing {len(remaining_names)} remaining names out of {len(names)} total")
    print(f"Processing {len(remaining_names)} names...")
    
    try:
        for name_info in tqdm(remaining_names, desc="🔄 Processing Names"):
            fetch_and_process_name(name_info, processed_names)
            time.sleep(SEARCH_DELAY)
        
        print(f"✅ All names processed successfully! Processed {len(processed_names)} names.")
        logging.info("🎯 Completed all searches!")
    except KeyboardInterrupt:
        print("⚠️ Process interrupted by user!")
        logging.warning("⚠️ Process interrupted by user!")
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        logging.error(f"❌ Error in main execution: {e}")

if __name__ == "__main__":
    main()

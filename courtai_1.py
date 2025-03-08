import os
import time
import shutil
import multiprocessing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# 🔹 Configuration
BASE_DIRECTORY = os.path.expanduser("~/courtAI/Dockets")

# 🔹 Judicial District Mapping
JUDICIAL_DISTRICTS = {
    "4": "4th_district.csv",
    "10": "10th_district.csv",
    "11": "11th_district.csv"
}

# 🔹 Delete all existing files in the main Dockets directory
def delete_all_files_in_dockets():
    """Ensures a clean start by deleting any old files in the main `Dockets/` folder."""
    print("\n🗑️ Deleting all files in Dockets folder before running...\n")
    
    for item in os.listdir(BASE_DIRECTORY):
        item_path = os.path.join(BASE_DIRECTORY, item)

        try:
            if os.path.isfile(item_path):  # Delete files only, not folders
                os.remove(item_path)
                print(f"🗑️ Deleted file: {item}")
        except Exception as e:
            print(f"⚠️ WARNING: Could not delete {item}: {e}")

# 🔹 Ensure a unique district folder exists for safe downloading
def ensure_district_folder(district_name):
    """Creates a district-specific folder inside `Dockets/` if it doesn't exist."""
    district_folder = os.path.join(BASE_DIRECTORY, district_name)
    os.makedirs(district_folder, exist_ok=True)
    print(f"📂 Ensured folder exists: {district_folder}")
    return district_folder

# 🔹 Wait for a file to download completely in a specific folder
def wait_for_new_file(directory, timeout=90):
    """Waits for a file to be fully downloaded and stops changing before renaming."""
    print(f"⏳ Waiting for a file to download in {directory}...")
    existing_files = set(os.listdir(directory))
    end_time = time.time() + timeout

    while time.time() < end_time:
        current_files = set(os.listdir(directory))
        new_files = list(current_files - existing_files)

        for file in new_files:
            if file.startswith("docket_search_results") and file.endswith(".csv"):
                file_path = os.path.join(directory, file)

                # Ensure file is fully written before renaming
                prev_size = -1
                stable_count = 0
                while stable_count < 5:  # Wait for file to remain unchanged for 5 cycles
                    if os.path.exists(file_path):
                        current_size = os.path.getsize(file_path)
                        if current_size == prev_size:
                            stable_count += 1
                        else:
                            stable_count = 0
                        prev_size = current_size
                    time.sleep(2)

                print(f"✅ File detected and fully written: {file_path}")
                return file_path

        time.sleep(1)

    print(f"❌ No new file detected in {directory} within timeout!")
    return None

# 🔹 Selenium - Process Each Judicial District
def process_district(district_value, filename):
    """Handles downloading data for a single judicial district in a separate process."""
    print(f"🚀 [{time.strftime('%H:%M:%S')}] Starting process for district {district_value}")

    # Ensure the download folder exists
    district_folder = ensure_district_folder(f"{district_value}_district")

    # Configure Chrome for isolated downloads
    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": district_folder}  # Each process gets its own download folder
    options.add_experimental_option("prefs", prefs)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--new-window")

    driver = webdriver.Chrome(options=options)
    driver.get("https://www.coloradojudicial.gov/dockets")

    try:
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "edit-district")))

        Select(WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.ID, "edit-district")))).select_by_value(district_value)
        print(f"✅ District set to value '{district_value}'.")

        WebDriverWait(driver, 30).until_not(EC.presence_of_element_located((By.CSS_SELECTOR, ".ajax-progress")))

        driver.execute_script("document.querySelector('#edit-date-fieldset').setAttribute('open','true');")
        Select(WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.ID, "edit-date-range")))).select_by_visible_text("1 Month")

        driver.execute_script("document.querySelector('#edit-case-number-fieldset').setAttribute('open','true');")
        Select(WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.ID, "edit-case-class")))).select_by_visible_text("CR")

        find_dockets_button = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.ID, "edit-submit")))
        find_dockets_button.click()

        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "download-docket-search-results-button")))
        print(f"✅ Results loaded for '{district_value}'.")

        time.sleep(3)  # Extra wait to prevent race conditions
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.ID, "download-docket-search-results-button"))).click()
        print(f"📂 Exporting results for '{district_value}'.")

        # Wait for the file to download inside the specific folder
        downloaded_file = wait_for_new_file(district_folder, timeout=90)

        if downloaded_file:
            new_path = os.path.join(BASE_DIRECTORY, filename)
            os.rename(downloaded_file, new_path)
            time.sleep(2)

            if os.path.exists(new_path):
                print(f"✅ File saved as '{filename}' for '{district_value}'.")
            else:
                print(f"❌ ERROR: {filename} is missing right after rename!")

        else:
            print(f"❌ CRITICAL ERROR: File for '{district_value}' failed to download.")
            return  # Prevents folder deletion if file failed

    except Exception as e:
        print(f"❌ ERROR in district '{district_value}': {e}")

    finally:
        driver.quit()
        print(f"✅ Process completed for district {district_value}")

        # 🔹 Delete district-specific folder after processing
        if os.path.exists(district_folder) and len(os.listdir(district_folder)) == 0:
            try:
                shutil.rmtree(district_folder)
                print(f"🗑️ Deleted district folder: {district_folder}")
            except Exception as e:
                print(f"⚠️ WARNING: Could not delete folder {district_folder}: {e}")

# 🔹 Main Execution
if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")

    # 🔹 **Delete all existing files before starting the process**
    delete_all_files_in_dockets()

    processes = [multiprocessing.Process(target=process_district, args=(d, f)) for d, f in JUDICIAL_DISTRICTS.items()]
    
    for p in processes:
        p.start()
    for p in processes:
        p.join()

    print("✅ All districts processed successfully!")

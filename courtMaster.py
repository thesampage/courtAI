import subprocess
import logging
import sys
import os

# Configure logging
log_file = "/Users/sampage/courtAI/master_script.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')

# List of scripts to run in order
scripts = [
    '/Users/sampage/courtAI/courtai_1.py',
    '/Users/sampage/courtAI/courtai_2.py',
    '/Users/sampage/courtAI/courtAI_3.py',
    '/Users/sampage/courtAI/courtAI_4.py',
    '/Users/sampage/courtAI/courtAI_5.py',
]
def run_scripts_in_terminal():
    """Run scripts sequentially directly in the terminal."""
    for script in scripts:
        print(f"Running {script}...\n")
        logging.info(f"Running {script}...")
        try:
            # Execute the script directly in the terminal
            subprocess.run(['python3', script], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error: {script} failed with return code {e.returncode}. Exiting process.\n")
            logging.error(f"{script} failed with return code {e.returncode}. Exiting process.")
            sys.exit(1)  # Exit the entire process on failure
        except Exception as e:
            print(f"Unexpected error: {e}. Exiting process.\n")
            logging.error(f"Unexpected error with {script}: {e}")
            sys.exit(1)  # Exit the entire process on unexpected errors

    print("All scripts ran successfully.")
    logging.info("All scripts ran successfully.")

if __name__ == "__main__":
    # Clear the log file for a fresh start
    with open(log_file, 'w'):
        pass
    run_scripts_in_terminal()

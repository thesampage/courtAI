import pandas as pd
import os
import sys

# Define directories
download_directory = "/Users/sampage/courtAI/Dockets"
output_file = os.path.join(download_directory, "docket_codeready.csv")
log_file = os.path.join(download_directory, "exclusion_log.txt")  # Log file for exclusions

# Define the CSV file names
csv_files = [
    "4th_district.csv",
    "10th_district.csv",
    "11th_district.csv"
]

# Step 1: Read and validate each CSV file
dfs = []
for file in csv_files:
    file_path = os.path.join(download_directory, file)

    # 🔴 ERROR: If any file is missing, terminate the script
    if not os.path.exists(file_path):
        print(f"❌ CRITICAL ERROR: Required file '{file}' not found. Terminating script.")
        sys.exit(1)

    try:
        df = pd.read_csv(file_path)

        # Ensure required columns exist
        required_columns = {"Date", "Time", "Name", "Case Number", "Hearing Type", "Location"}
        missing_columns = required_columns - set(df.columns)
        
        # 🔴 ERROR: If any required column is missing, terminate the script
        if missing_columns:
            print(f"❌ CRITICAL ERROR: File '{file}' is missing required columns: {missing_columns}. Terminating script.")
            sys.exit(1)

        df['Source File'] = file  # Keep track of source file
        dfs.append(df)

    except Exception as e:
        print(f"❌ CRITICAL ERROR: Failed to read '{file}' due to: {e}. Terminating script.")
        sys.exit(1)

# Step 2: Merge all DataFrames (if we reach this point, all files are valid)
merged_df = pd.concat(dfs, ignore_index=True)

# Step 3: Identify rows that will be excluded based on 'Hearing Type'
hearing_types_to_exclude = [
    "Review WAppearance of Parties", "Hearing on Bond", "HrgRevocation of Probation", 
    "Review Hearing", "Compliance Hrg DV Relinquish", "Appearance on Arrest Warrant", 
    "Rttn on Summ for Rev of Prob", "Appearance of Counsel", "Status Conference", 
    "Appearance on Bond", "Rtrn Filing of Charges", "HrgRevocation of Deferred", 
    "Hearing on Petition to Seal", "Review Hearing", "Show Cause Hearing", "Setting", "Hearing on Probation",
    "PreTrial Readiness Conference", "Restitution Hearing", "Rtrn on Summ for Rev of Prob"
]

excluded_rows = merged_df[merged_df['Hearing Type'].isin(hearing_types_to_exclude)]
filtered_df = merged_df[~merged_df['Hearing Type'].isin(hearing_types_to_exclude)]

# Step 4: Handle multiple cases for the same name, date, and time
grouped_df = filtered_df.groupby(['Date', 'Time', 'Name', 'Location', 'Hearing Type']).agg({
    'Case Number': lambda x: ', '.join(x.dropna().astype(str).str.strip())  # Clean & merge case numbers
}).reset_index()

# Step 5: Ensure the correct column order
expected_columns = ['Date', 'Time', 'Name', 'Case Number', 'Hearing Type', 'Location']
grouped_df = grouped_df[expected_columns]  # Reorder

# Step 6: Save the cleaned file
grouped_df.to_csv(output_file, index=False)
print(f"✅ SUCCESS: Merged and cleaned data saved to: {output_file}")

# Step 7: Log excluded names and reasons
if not excluded_rows.empty:
    with open(log_file, "w") as log:
        log.write("Excluded Names and Reasons:\n")
        log.write("=" * 50 + "\n")
        for _, row in excluded_rows.iterrows():
            log.write(f"Name: {row['Name']}, Case Number: {row['Case Number']}, "
                      f"Hearing Type: {row['Hearing Type']} (Excluded)\n")

    print(f"📄 Log file created: {log_file}")
else:
    print("✅ No exclusions detected.")

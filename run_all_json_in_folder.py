import subprocess
import os
import glob

# Folder path where JSON files are located
json_folder = r"C:\Files\Git\tableau-automation-scripts\US_PROD_Schema\temp"

# Script to execute for each JSON file
script_to_run = "packge_schema_deploy.py"

# Find all .json files in the folder (non-recursively)
json_files = glob.glob(os.path.join(json_folder, "*.json"))

# Sort the list if you want consistent execution order
json_files.sort()

# Run each file
for json_file in json_files:
    print(f"\n🔄 Running: {script_to_run} {json_file}")
    result = subprocess.run(["python", script_to_run, json_file])
    if result.returncode != 0:
        print(f"❌ Failed on: {json_file}")
        break
    else:
        print(f"✅ Success: {json_file}")

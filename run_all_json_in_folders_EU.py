import subprocess
import os
import glob

# Dynamically determine the path to the "EU_PROD_Schema/temp" folder inside the repo
repo_root = os.path.dirname(os.path.abspath(__file__))
json_folder = os.path.join(repo_root, "EU_PROD_Schema", "temp")

# Script to execute for each JSON file
script_to_run = os.path.join(repo_root, "packge_schema_deploy_EU.py")

# Find all .json files in the folder (non-recursively)
json_files = glob.glob(os.path.join(json_folder, "*.json"))
json_files.sort()

# Run each file
for json_file in json_files:
    print(f"\nRunning: {script_to_run} {json_file}")
    result = subprocess.run(["python3", script_to_run, json_file])
    if result.returncode != 0:
        print(f"Failed on: {json_file}")
        break
    else:
        print(f"Success: {json_file}")

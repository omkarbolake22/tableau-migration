import tableauserverclient as TSC
import json
import pandas as pd
import os

# -----------------------------
# Configuration
# -----------------------------
CONFIG_FILE = "tableau_config.json"
ENVIRONMENT = "QA"  # Switch between environments if needed

# -----------------------------
# Load Tableau Server Config
# -----------------------------
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)[ENVIRONMENT]

server_url = config["server_url"]
token_name = config["token_name"]
personal_access_token = config["personal_access_token"]
site_name = config["site_name"]

# -----------------------------
# Connect to Tableau Server
# -----------------------------
tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=token_name,
    personal_access_token=personal_access_token,
    site_id=site_name
)

server = TSC.Server(server_url, use_server_version=True)

# -----------------------------
# Fetch and Save Jobs
# -----------------------------
with server.auth.sign_in(tableau_auth):
    print("✅ Connected to Tableau Server:", server_url)

    req_options = TSC.RequestOptions(pagesize=1000)
    jobs, pagination = server.jobs.get(req_options)

    job_data = [{
        "Job ID": job.id,
        "Status": job.status,
        "Task Type": job.type,
        "Progress": job.progress,
        "Created At": job.created_at,
        "Started At": job.started_at,
        "Ended At": job.ended_at,
        "Notes": job.notes
    } for job in jobs]

    df = pd.DataFrame(job_data)
    output_file = "tableau_jobs.csv"
    df.to_csv(output_file, index=False)
    print(f"📄 Job data saved to: {output_file}")

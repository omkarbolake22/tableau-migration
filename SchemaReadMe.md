# Tableau Athena Schema Automation

This repository provides automation scripts to deploy Athena schemas and views for Tableau, using JSON configuration files. You can run deployments for all JSON files in a folder or a single configuration file.

---

## How to Use

### 1. Prepare Your JSON Config Files

- Place your schema configuration JSON files in a folder (e.g., `US_PROD_Schema/`).
- Each JSON file should define the schema deployment parameters.  
  See the sample: `US_PROD_Schema/course_schema_group1a.json`.

### 2. Batch Deploy: Run All JSON Files in a Folder

Use `run_all_json_in_folder.py` to process every `.json` file in a folder.

**Steps:**
1. Open `run_all_json_in_folder.py`.
2. Set the folder path:
   ```python
   json_folder = r"C:\Files\Git\tableau-automation-scripts\US_PROD_Schema"
   ```
   Change this path to your folder if needed.
3. Run the script:
   ```sh
   python run_all_json_in_folder.py
   ```
   - The script will execute `packge_schema_deploy.py` for each JSON file in the folder.
   - If any deployment fails, the batch process stops.

### 3. Deploy a Single Schema

You can also run the deployment for a single JSON config file:

```sh
python packge_schema_deploy.py US_PROD_Schema\course_schema_group1a.json
```
- Replace the path with your specific JSON file.

---

## Configuration Example

A sample JSON config (`course_schema_group1a.json`):

```json
{
  "customer_group_identifier": "pstest_customer_group_001",
  "target_schema_name": "analake_course_group1_db",
  "source_schema_name": "analake_catalog_db",
  "where_condition": "site_name in('lcecpartner6','qatracking06','pstest','catalyst04')",
  "base_tables": [
    "v_company_displayname",
    "v_completion_kpis_benchmark_tblu_final"
    // ... more tables ...
  ],
  "tags": {},
  "schema_description": "Schema for PSTest Customer Group containing filtered views.",
  "connection": {
    "class": "athena",
    "server_endpoint_url": "...",
    "s3_output_location": "...",
    "access_key": "",
    "secret_key": "",
    "aws_region": "us-east-1",
    "workgroup": "primary"
  }
}
```

---

## Notes

- **Folder Path:**  
  Change the `json_folder` variable in `run_all_json_in_folder.py` to point to your folder of JSON files.
- **Single File:**  
  You can run `packge_schema_deploy.py` directly with any JSON config file.
- **AWS Credentials:**  
  Make sure your environment is configured with the necessary AWS credentials and permissions.
- **Error Handling:**  
  The batch script stops on the first failure and prints an error message.

---

## File Overview

- `run_all_json_in_folder.py` — Batch runner for all JSON configs in a folder.
- `packge_schema_deploy.py` — Deploys a schema as described in a single JSON config.
- `US_PROD_Schema/` — Folder containing your JSON configuration files.

---

## Troubleshooting

- Ensure all JSON files are valid and contain required fields.
- Check AWS credentials and permissions if you encounter authentication or access errors.
- Review log output for details on any failures.

---
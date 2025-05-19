import boto3
import json
import time
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

AWS_REGION = "us-east-1"
S3_ATHENA_OUTPUT_LOCATION = "s3://aws-athena-query-results-us-east-1-180350466832/Reveal-TableauCloud/" # From your logs
ATHENA_WORKGROUP = "primary"

def load_config(config_path):
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logging.info(f"Configuration loaded successfully from {config_path}")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in configuration file: {config_path}")
        raise

def execute_athena_query(athena_client, query, database_name=None):
    logging.info(f"Executing Athena Query: {query[:300]}{'...' if len(query) > 300 else ''}")
    query_execution_context = {}
    if database_name:
        query_execution_context['Database'] = database_name

    query_execution_id = None
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext=query_execution_context,
            ResultConfiguration={'OutputLocation': S3_ATHENA_OUTPUT_LOCATION},
            WorkGroup=ATHENA_WORKGROUP
        )
        query_execution_id = response['QueryExecutionId']
        logging.info(f"Query '{query_execution_id}' started.")

        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if status == 'SUCCEEDED':
                    logging.info(f"Query '{query_execution_id}' SUCCEEDED.")
                else:
                    error_message = stats['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logging.error(f"Query '{query_execution_id}' {status}. Reason: {error_message}")
                    raise Exception(f"Athena query {status}: {error_message}")
                break
            logging.debug(f"Query '{query_execution_id}' status: {status}. Waiting...")
            time.sleep(5)
        return query_execution_id
    except Exception as e:
        logging.error(f"Error executing Athena query (ID: {query_execution_id if query_execution_id else 'N/A'}): {e}")
        raise

def check_schema_exists(glue_client, schema_name):
    try:
        glue_client.get_database(Name=schema_name)
        logging.info(f"Schema '{schema_name}' (in awsdatacatalog) already exists.")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        logging.info(f"Schema '{schema_name}' (in awsdatacatalog) does not exist.")
        return False
    except Exception as e:
        logging.error(f"Error checking schema '{schema_name}' (in awsdatacatalog): {e}")
        raise

# --- MODIFIED FUNCTION with simplified ARN fetch ---
def create_schema_with_glue(glue_client, athena_client, schema_name, description, tags):
    """
    Creates an Athena schema (Glue Database) using Glue API.
    Tags are applied in a separate call if provided and ARN is retrieved promptly.
    """
    if check_schema_exists(glue_client, schema_name):
        logging.warning(f"Schema '{schema_name}' already exists. Skipping creation and tagging.")
        return

    logging.info(f"Attempting to create schema '{schema_name}' (in awsdatacatalog) via Glue API.")
    database_arn = None
    try:
        glue_client.create_database(
            DatabaseInput={ # CatalogId implicitly current account
                'Name': schema_name,
                'Description': description
            }
        )
        logging.info(f"Schema '{schema_name}' created successfully via Glue API.")

        # Attempt to get ARN for tagging after a short delay
        logging.info(f"Waiting 10 seconds for Glue eventual consistency before attempting ARN retrieval for '{schema_name}'...")
        time.sleep(10) # Wait 10 seconds

        try:
            logging.info(f"Attempting to retrieve ARN for database '{schema_name}' for tagging.")
            db_details = glue_client.get_database(Name=schema_name) # CatalogId implicitly current account
            database_arn = db_details['Database'].get('ARN')
            if database_arn:
                logging.info(f"Successfully retrieved ARN for '{schema_name}': {database_arn}")
            else:
                logging.warning(f"ARN not found in get_database response for '{schema_name}' after delay. Skipping tagging.")
        except Exception as e:
            logging.warning(f"Error retrieving database ARN for '{schema_name}' after delay, cannot apply tags: {e}")
            database_arn = None # Ensure it's None if retrieval failed


        # Apply tags if they are provided and the database_arn was obtained
        if tags and database_arn:
            logging.info(f"Attempting to apply tags to schema '{schema_name}' (ARN: {database_arn}): {tags}")
            glue_client.tag_resource(ResourceArn=database_arn, TagsToAdd=tags)
            logging.info(f"Tags successfully applied to schema '{schema_name}'.")
        elif not tags:
            logging.info(f"No tags provided for schema '{schema_name}'. Skipping tagging.")
        elif not database_arn and tags: # If tags were provided but ARN is missing
            logging.warning(f"Tags were provided for '{schema_name}', but its ARN could not be determined after delay. Tags NOT applied.")


        # Athena recognition part
        try:
            execute_athena_query(athena_client, f'SHOW TABLES IN "{schema_name}"', database_name=schema_name)
            logging.info(f"Athena successfully recognized the new schema '{schema_name}'.")
        except Exception as e:
            logging.warning(f"Athena had a minor hiccup recognizing schema '{schema_name}' immediately after creation: {e}. Proceeding...")

    except glue_client.exceptions.AlreadyExistsException:
        logging.warning(f"Schema '{schema_name}' already exists (detected during Glue create_database call).")
    except Exception as e:
        logging.error(f"Error during schema '{schema_name}' creation or tagging process: {e}")
        raise
# --- END OF MODIFIED FUNCTION ---

def main(config_file_path):
    if not S3_ATHENA_OUTPUT_LOCATION or "your-aws-athena-query-results-bucket" in S3_ATHENA_OUTPUT_LOCATION:
        logging.warning(f"S3_ATHENA_OUTPUT_LOCATION ('{S3_ATHENA_OUTPUT_LOCATION}') might not be correctly configured with your bucket. Please verify.")

    try:
        config = load_config(config_file_path)
    except Exception:
        return

    target_schema = config['target_schema_name']
    source_schema = config['source_schema_name']
    where_condition = config.get('where_condition', "").strip()
    base_tables = config.get('base_tables', [])
    if not isinstance(base_tables, list):
        logging.error(f"'base_tables' in config file must be a list. Found: {type(base_tables)}")
        return

    tags = config.get('tags', {})
    schema_description = config.get('schema_description', f"Schema for {config.get('customer_group_identifier', 'N/A')}")

    try:
        athena_client = boto3.client('athena', region_name=AWS_REGION)
        glue_client = boto3.client('glue', region_name=AWS_REGION)
        logging.info(f"AWS clients initialized for region {AWS_REGION}")
    except Exception as e:
        logging.error(f"Failed to initialize AWS clients: {e}")
        return

    try:
        create_schema_with_glue(glue_client, athena_client, target_schema, schema_description, tags)
    except Exception as e:
        logging.error(f"Halting script due to error during schema creation for '{target_schema}': {e}")
        return

    if not base_tables:
        logging.info("No base tables specified in the configuration. Skipping view creation.")
    else:
        logging.info(f"Starting view creation process for target schema '{target_schema}'. Number of tables: {len(base_tables)}")
        for simple_table_name in base_tables:
            view_name_for_log = f"awsdatacatalog.{target_schema}.{simple_table_name}"
            source_object_for_log = f"awsdatacatalog.{source_schema}.{simple_table_name}"

            logging.info(f"Processing: CREATE VIEW {view_name_for_log} AS SELECT * FROM {source_object_for_log}")

            view_ddl_core = f"""SELECT * FROM "awsdatacatalog"."{source_schema}"."{simple_table_name}" """

            if where_condition:
                view_ddl = f"""CREATE OR REPLACE VIEW "awsdatacatalog"."{target_schema}"."{simple_table_name}" AS {view_ddl_core} WHERE {where_condition}"""
                logging.info(f"View will be created with WHERE clause: {where_condition}")
            else:
                view_ddl = f"""CREATE OR REPLACE VIEW "awsdatacatalog"."{target_schema}"."{simple_table_name}" AS {view_ddl_core}"""
                logging.info("View will be created without a WHERE clause.")

            try:
                execute_athena_query(athena_client, view_ddl, database_name=target_schema)
                logging.info(f"View '{view_name_for_log}' created/replaced successfully.")
            except Exception as e:
                logging.error(f"Failed to create/replace view '{view_name_for_log}': {e}")

    logging.info("Athena schema and view management script completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Athena Schema & View Management Script (gluecreds.py)")
    parser.add_argument("config_file_path", help="Path to the JSON configuration file (e.g., schema_request.json)")
    args = parser.parse_args()
    main(args.config_file_path)
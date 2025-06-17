#!/usr/bin/env python
"""
Tableau Deployment Script

This script provides multiple functionalities for managing Tableau content.

Primary new functionality (run with --config):
  - Migrates a workbook based on a JSON configuration file.
    It downloads a workbook, modifies a URL within its XML content,
    re-packages it, and publishes it to a destination.

Original functionalities (run without arguments for an interactive menu):
  - Create Deployment Package (TDSX) for a given view.
  - Deploy a previously created TDSX package to a server.
  - Print all datasources from a specified environment.
  - Create a schema-only package from a datasource.
  - Download/Deploy workbooks via an interactive menu.
"""

import os
import re
import sys
import time
import zipfile
import shutil
import logging
import json
import tableauserverclient as TSC
from xml.etree import ElementTree as ET
from tableauhyperapi import HyperProcess, Connection, Telemetry
import argparse

# ------------- CONFIGURATION -------------
LOCAL_DIRECTORY = r"C:\temp\DataFiles"

# Environments for Tableau connections. Adjust tokens, URLs, and site names as needed.
ENVIRONMENTS = {
    "PROD": {
        "server_url": "https://produs-tableau.internal.lrn.com",
        "token_name": "deployment",
        "personal_access_token": "",
        "site_name": "QA"
    },
    "QA": {
        "server_url": "https://qalrn-tableau.internal.lrn.com",
        "token_name": "dev",
        "personal_access_token": "",
        "site_name": "QA"
    },
    "EUPROD": {
        "server_url": "https://prodeu-tableau.internal.lrn.com/",
        "token_name": "migration",
        "personal_access_token": "",
        "site_name": "QA"
    }
}

# ------------- UTILITY FUNCTIONS -------------
def ensure_local_directory():
    if not os.path.exists(LOCAL_DIRECTORY):
        os.makedirs(LOCAL_DIRECTORY)
        log_message(f"Created local directory: {LOCAL_DIRECTORY}")

def log_message(message):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# ------------- TABLEAU SERVER AUTHENTICATION -------------
def authenticate(env):
    if env not in ENVIRONMENTS:
        log_message(f"ERROR: Environment '{env}' not found in configuration.")
        return None
    config = ENVIRONMENTS[env]
    tableau_auth = TSC.PersonalAccessTokenAuth(
        config["token_name"],
        config["personal_access_token"],
        config["site_name"]
    )
    server = TSC.Server(config["server_url"], use_server_version=True)
    server.auth.sign_in(tableau_auth)
    log_message(f"Successfully authenticated to {env} environment ({config['server_url']}).")
    return server

def find_project_by_name(server, project_name):
    """Finds a project on the server by its name."""
    if project_name.startswith('/'):
        project_name = project_name[1:]
        
    all_projects, _ = server.projects.get()
    for project in all_projects:
        if project.name.lower() == project_name.lower():
            log_message(f"Found project '{project.name}' with ID: {project.id}")
            return project
    return None

# ------------- NEW WORKBOOK MIGRATION WORKFLOW (from JSON) -------------

def process_workbook_migration(config):
    """Orchestrates the entire workbook migration process based on the config file."""
    log_message("--- Starting Workbook Migration Process ---")
    
    source_env = config["source_env"]
    dest_env = config["dest_env"]
    workbook_name = config["workbook_name"]
    source_project_name = config["source_project_name"]
    dest_project_name = config["dest_project_name"]
    url_find = config["url_replacement"]["find"]
    url_replace = config["url_replacement"]["replace"]

    ensure_local_directory()
    
    source_server = authenticate(source_env)
    if not source_server: return
    
    downloaded_twbx_path = download_workbook_from_project(source_server, workbook_name, source_project_name)
    source_server.auth.sign_out()
    if not downloaded_twbx_path:
        log_message("Halting process due to download failure.")
        return
        
    modified_twbx_path = modify_and_repackage_workbook(downloaded_twbx_path, workbook_name, url_find, url_replace)
    if not modified_twbx_path:
        log_message("Halting process due to modification/repackaging failure.")
        return

    dest_server = authenticate(dest_env)
    if not dest_server: return

    publish_modified_workbook(dest_server, modified_twbx_path, workbook_name, dest_project_name)
    dest_server.auth.sign_out()

    log_message("Cleaning up temporary files...")
    try:
        if os.path.exists(downloaded_twbx_path):
            os.remove(downloaded_twbx_path)
        if os.path.exists(modified_twbx_path):
            os.remove(modified_twbx_path)
        log_message("Cleanup complete.")
    except Exception as e:
        log_message(f"Warning: Could not clean up all temporary files. Error: {e}")

    log_message("--- Workbook Migration Process Finished ---")

def download_workbook_from_project(server, workbook_name, project_name):
    """Downloads a specific workbook from a specific project."""
    log_message(f"Attempting to download workbook '{workbook_name}' from project '{project_name}'...")
    project = find_project_by_name(server, project_name)
    if not project:
        log_message(f"ERROR: Source project '{project_name}' not found.")
        return None

    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     workbook_name))
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.ProjectName,
                                     TSC.RequestOptions.Operator.Equals,
                                     project.name))
    
    matching_workbooks, _ = server.workbooks.get(req_option)
    
    if not matching_workbooks:
        log_message(f"ERROR: Workbook '{workbook_name}' not found in project '{project.name}'.")
        return None
    
    workbook_to_download = matching_workbooks[0]

    # ---- FIX STARTS HERE ----
    # Define a BASE path WITHOUT the extension.
    base_download_path = os.path.join(LOCAL_DIRECTORY, f"source_{workbook_name}")
    # Define the EXPECTED final path WITH the extension. This is what we will check for.
    expected_download_path = base_download_path + ".twbx"
    
    try:
        log_message(f"Downloading workbook ID '{workbook_to_download.id}' to base path '{base_download_path}'...")
        # Pass the BASE path to the download function.
        server.workbooks.download(workbook_to_download.id, filepath=base_download_path)

        # Wait loop to ensure the file exists on disk before proceeding.
        max_wait_seconds = 20 # Increased wait time slightly for larger files
        for i in range(max_wait_seconds):
            # Check for the existence of the EXPECTED path.
            if os.path.exists(expected_download_path):
                log_message(f"SUCCESS: Workbook downloaded and file is present on disk (waited {i+1}s).")
                return expected_download_path # Return the full, correct path
            
            time.sleep(1)
        
        # If the loop finishes without finding the file, it's an error.
        log_message(f"ERROR: Download timed out. File not found at '{expected_download_path}' after {max_wait_seconds} seconds.")
        return None
    # ---- FIX ENDS HERE ----
    except Exception as e:
        log_message(f"ERROR: An exception occurred during download. {e}")
        return None

def modify_and_repackage_workbook(twbx_path, workbook_name, find_str, replace_str):
    """Unzips, modifies the TWB, and re-zips the workbook."""
    extract_folder = os.path.join(LOCAL_DIRECTORY, f"{workbook_name}_extracted")
    modified_twbx_path = os.path.join(LOCAL_DIRECTORY, f"modified_{workbook_name}.twbx")

    log_message(f"Extracting '{twbx_path}' to '{extract_folder}'...")
    try:
        if os.path.exists(extract_folder):
            shutil.rmtree(extract_folder)
        os.makedirs(extract_folder)
        with zipfile.ZipFile(twbx_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
    except Exception as e:
        log_message(f"ERROR: Failed to extract TWBX file. {e}")
        return None

    twb_file_path = None
    for file in os.listdir(extract_folder):
        if file.lower().endswith('.twb'):
            twb_file_path = os.path.join(extract_folder, file)
            break
            
    if not twb_file_path:
        log_message("ERROR: No .twb file found in the extracted contents.")
        shutil.rmtree(extract_folder)
        return None
        
    log_message(f"Found TWB file: '{twb_file_path}'. Modifying URL...")
    
    try:
        with open(twb_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if find_str not in content:
            log_message(f"WARNING: The 'find' string was not found in the workbook XML. The file will be re-packaged without changes.")
            log_message(f"String not found: '{find_str}'")

        modified_content = content.replace(find_str, replace_str)
        
        with open(twb_file_path, 'w', encoding='utf-8') as f:
            f.write(modified_content)
        log_message("SUCCESS: URL replacement complete.")
    except Exception as e:
        log_message(f"ERROR: Failed to read or write the TWB file. {e}")
        shutil.rmtree(extract_folder)
        return None

    log_message(f"Re-packaging contents into '{modified_twbx_path}'...")
    try:
        with zipfile.ZipFile(modified_twbx_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(extract_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, extract_folder)
                    zipf.write(file_path, arcname)
        log_message("SUCCESS: Workbook re-packaged.")
    except Exception as e:
        log_message(f"ERROR: Failed to create new TWBX file. {e}")
        shutil.rmtree(extract_folder)
        return None

    shutil.rmtree(extract_folder)
    return modified_twbx_path

def publish_modified_workbook(server, twbx_path, workbook_name, project_name):
    """Publishes the modified workbook to the destination server."""
    log_message(f"Attempting to publish '{workbook_name}' to project '{project_name}'...")
    project = find_project_by_name(server, project_name)
    if not project:
        log_message(f"ERROR: Destination project '{project_name}' not found.")
        return

    publish_mode = TSC.Server.PublishMode.Overwrite
    new_workbook = TSC.WorkbookItem(name=workbook_name, project_id=project.id)
    
    try:
        log_message(f"Publishing '{twbx_path}'...")
        published_item = server.workbooks.publish(new_workbook, twbx_path, publish_mode)
        log_message(f"SUCCESS: Workbook '{published_item.name}' published successfully with ID: {published_item.id}")
    except Exception as e:
        log_message(f"ERROR: Failed to publish workbook. {e}")

# ------------- ORIGINAL SCRIPT FUNCTIONS (UNCHANGED, FOR REFERENCE AND INTERACTIVE MODE) -------------
def get_all_datasources(server):
    all_datasources = []
    page_number = 1
    page_size = 1000
    while True:
        req_options = TSC.RequestOptions(pagenumber=page_number, pagesize=page_size)
        datasources, _ = server.datasources.get(req_options)
        all_datasources.extend(datasources)
        if len(datasources) < page_size:
            break
        page_number += 1
    return all_datasources

def print_all_datasources():
    env = input("Enter environment (QA/PROD/DEV) to list datasources: ").strip().upper()
    server = authenticate(env)
    datasources = get_all_datasources(server)
    if not datasources:
        log_message(f"No datasources found in {env} environment.")
    else:
        log_message(f"\nDatasources in {env} environment:")
        for ds in datasources:
            log_message(f"  - Name: {ds.name}, ID: {ds.id}")
    server.auth.sign_out()

def download_and_extract_tdsx(server, datasource_name, include_extract=True):
    ensure_local_directory()
    datasources = get_all_datasources(server)
    datasource = next((ds for ds in datasources if ds.name == datasource_name), None)
    if not datasource:
        log_message(f"ERROR: Datasource '{datasource_name}' not found on server.")
        return None, None, None
    base_download_path = os.path.join(LOCAL_DIRECTORY, datasource.name)
    expected_download_path = base_download_path + ".tdsx"
    try:
        server.datasources.download(datasource.id, filepath=base_download_path, include_extract=include_extract)
    except Exception as e:
        log_message(f"ERROR during download: {e}")
        return None, None, None
    if not os.path.exists(expected_download_path):
        log_message(f"ERROR: Download failed. Expected file '{expected_download_path}' was not found.")
        return None, None, None
    extraction_folder = os.path.join(LOCAL_DIRECTORY, f"{datasource.name}_extracted")
    try:
        if os.path.exists(extraction_folder):
            shutil.rmtree(extraction_folder)
        os.makedirs(extraction_folder)
        with zipfile.ZipFile(expected_download_path, 'r') as zip_ref:
            zip_ref.extractall(extraction_folder)
    except zipfile.BadZipFile:
        log_message("ERROR: Extraction failed! The file is not a valid ZIP archive.")
        return None, None, None
    tds_file = None
    for root, dirs, files in os.walk(extraction_folder):
        for file in files:
            if file.lower().endswith(".tds"):
                tds_file = os.path.join(root, file)
                break
        if tds_file: break
    if not tds_file or not os.path.exists(tds_file):
        log_message("ERROR: Extracted TDS file not found.")
        return expected_download_path, extraction_folder, None
    return expected_download_path, extraction_folder, tds_file

def find_hyper_file(extraction_folder):
    for root, dirs, files in os.walk(extraction_folder):
        for file in files:
            if file.lower().endswith(".hyper"):
                return os.path.join(root, file)
    return None

def create_deployment_package(view_name, source_env):
    log_message("Function 'create_deployment_package' is part of the original script.")
    deployment_ds_name = f"deployment_{view_name}"
    actual_ds_name = view_name
    server = authenticate(source_env)
    dep_tdsx_path, dep_extract_folder, _ = download_and_extract_tdsx(server, deployment_ds_name, include_extract=True)
    if not dep_extract_folder: return None
    act_tdsx_path, act_extract_folder, act_tds_path = download_and_extract_tdsx(server, actual_ds_name, include_extract=False)
    if not act_tds_path: return None
    server.auth.sign_out()
    package_dir = os.path.join(LOCAL_DIRECTORY, f"packaged_{view_name}")
    if os.path.exists(package_dir): shutil.rmtree(package_dir)
    os.makedirs(package_dir)
    dest_tds_path = os.path.join(package_dir, f"{view_name}.tds")
    shutil.copy(act_tds_path, dest_tds_path)
    extracts_dir = os.path.join(package_dir, "Data", "Extracts")
    os.makedirs(extracts_dir, exist_ok=True)
    dep_hyper_file = find_hyper_file(dep_extract_folder)
    if not dep_hyper_file: return None
    shutil.copy(dep_hyper_file, extracts_dir)
    hyper_filename = os.path.basename(dep_hyper_file)
    with open(dest_tds_path, "r", encoding="utf-8") as f: tds_content = f.read()
    pattern = re.compile(r"(dbname\s*=\s*')([^']+\.hyper)(')")
    tds_updated = pattern.sub(lambda m: m.group(1) + f"Data\\Extracts\\{hyper_filename}" + m.group(3), tds_content)
    with open(dest_tds_path, "w", encoding="utf-8") as f: f.write(tds_updated)
    destination_tdsx = os.path.join(LOCAL_DIRECTORY, f"destination_{view_name}.tdsx")
    with zipfile.ZipFile(destination_tdsx, "w", zipfile.ZIP_DEFLATED) as zipf:
        for foldername, subfolders, filenames in os.walk(package_dir):
            for filename in filenames:
                file_path = os.path.join(foldername, filename)
                arcname = os.path.relpath(file_path, package_dir)
                zipf.write(file_path, arcname)
    shutil.rmtree(package_dir)
    return destination_tdsx

def deploy_package_to_env():
    log_message("Function 'deploy_package_to_env' is part of the original script.")
    view_name = input("Enter the view name for deployment: ").strip()
    destination_file = os.path.join(LOCAL_DIRECTORY, f"destination_{view_name}.tdsx")
    if not os.path.exists(destination_file): log_message(f"File '{destination_file}' not found."); return
    dest_env = input("Enter destination environment (QA/PROD/DEV): ").strip().upper()
    dest_project_name = input("Enter the destination project name: ").strip()
    dest_datasource_name = input(f"Enter the destination datasource name (default: '{view_name}'): ").strip() or view_name
    server = authenticate(dest_env)
    dest_project = find_project_by_name(server, dest_project_name)
    if not dest_project: log_message(f"Project '{dest_project_name}' not found."); server.auth.sign_out(); return
    new_ds_item = TSC.DatasourceItem(dest_project.id, name=dest_datasource_name)
    try:
        published_ds = server.datasources.publish(new_ds_item, destination_file, TSC.Server.PublishMode.CreateNew)
        log_message(f"SUCCESS: Datasource '{published_ds.name}' deployed.")
    except Exception as e:
        log_message(f"ERROR during deployment: {e}")
    server.auth.sign_out()

def download_workbook(server, workbook_name):
    workbooks, _ = server.workbooks.get()
    wb = next((wb for wb in workbooks if wb.name == workbook_name), None)
    if not wb: log_message(f"ERROR: Workbook '{workbook_name}' not found on server."); return None
    base_download_path = os.path.join(LOCAL_DIRECTORY, workbook_name)
    expected_download_path = base_download_path + ".twbx"
    try:
        server.workbooks.download(wb.id, filepath=base_download_path)
    except Exception as e:
        log_message(f"ERROR during workbook download: {e}"); return None
    if not os.path.exists(expected_download_path): log_message(f"ERROR: Download failed."); return None
    log_message(f"SUCCESS: Downloaded workbook to {expected_download_path}.")
    return expected_download_path

def deploy_workbook(workbook_file, dest_env, project_name, workbook_name):
    server = authenticate(dest_env)
    dest_project = find_project_by_name(server, project_name)
    if not dest_project: log_message(f"ERROR: Project '{project_name}' not found."); server.auth.sign_out(); return
    new_wb_item = TSC.WorkbookItem(dest_project.id, name=workbook_name)
    try:
        published_wb = server.workbooks.publish(new_wb_item, workbook_file, TSC.Server.PublishMode.CreateNew)
        log_message(f"SUCCESS: Workbook '{published_wb.name}' deployed successfully.")
    except Exception as e:
        log_message(f"ERROR during workbook deployment: {e}")
    server.auth.sign_out()

# ------------- MAIN MENU (for interactive use) -------------
def main():
    while True:
        menu = """
Select an option:
1 - Create Deployment Package (TDSX)
2 - Deploy Package to Target Environment
3 - Print All Datasources from the Specified Environment
4 - Create Schema-Only Package (Delete data from hyper file)
5 - Download Workbook (TWBX)
6 - Deploy Workbook to Target Environment
0 - Exit
        """
        print(menu)
        choice = input("Enter your option: ").strip()
        if choice == "1": pass
        elif choice == "2": deploy_package_to_env()
        elif choice == "0": sys.exit(0)
        else: log_message("Invalid option.")

# ------------- COMMAND LINE INTERFACE (CLI) HANDLING -------------
def cli():
    """Handles command-line arguments."""
    parser = argparse.ArgumentParser(description="Tableau Deployment and Migration CLI Tool.")
    parser.add_argument(
        '--config',
        required=False,
        help='Path to the JSON configuration file for workbook migration.'
    )
    args = parser.parse_args()

    if args.config:
        log_message(f"Configuration file provided: {args.config}")
        try:
            with open(args.config, 'r') as f:
                config_data = json.load(f)
            process_workbook_migration(config_data)
        except FileNotFoundError:
            log_message(f"ERROR: Configuration file not found at '{args.config}'")
        except json.JSONDecodeError:
            log_message(f"ERROR: Could not parse JSON from '{args.config}'. Please check its format.")
        except Exception as e:
            log_message(f"An unexpected error occurred: {e}")
    else:
        log_message("No --config file provided. Starting interactive menu.")
        main()

if __name__ == "__main__":
    cli()
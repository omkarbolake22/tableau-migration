# Tableau Automation Scripts

This repository contains Python scripts to automate Tableau tasks such as downloading, deploying, and managing Tableau workbooks and datasources.

## Features

- **Download Workbooks**: Download Tableau workbooks from the server.
- **Deploy Workbooks**: Deploy workbooks to specific projects in Tableau Server.
- **List Datasources**: Extract and list datasources used in a workbook.
- **Update Datasources**: Update datasource references in Tableau workbooks.
- **Environment Management**: Manage Tableau environments (DEV, QA, PROD) with pre-configured credentials.

## Prerequisites

- Python 3.8 or higher
- Tableau Server Client (`tableauserverclient`)
- Tableau Hyper API (`tableauhyperapi`)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/tableau-automation-scripts.git
   cd tableau-automation-scripts
# Glamira User Behavior Transformation
This project uses dbt (data build tool) to transform and model data related to user interactions and behaviors on the Glamira platform.

## Table of Contents
- [Project Overview](#project-overview)
- [Setup](#setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [Testing](#testing)
- [Resources](#resources)

## Project Overview

This dbt project aims to transform raw user behavior data into insightful models for analysis. The project includes models for understanding user interactions, engagement, and other key metrics on Glamira.

### Key Features:
- Data models to analyze user interactions and behavior on the Glamira platform.
- Transformation scripts to clean and structure raw data.
- Automated testing to ensure data quality.

## Setup
1. **Set Up the Environment**:
If you haven't set up dbt, you can install it and initialize the environment using the following command:
    ```bash
    dbt init
    ```
   This command will set up the dbt environment and create the necessary directories and files.

2. **Configure Profile for BigQuery**:
After setting up dbt, configure your environment to use BigQuery as the data warehouse. Create or update your `profiles.yml` file in the `.dbt` directory in your home folder:
 ```yaml
    glamira_project:
      target: dev
      outputs:
        dev:
          type: bigquery
          method: oauth
          project: your-gcp-project-id
          dataset: your_dataset_name
          threads: 1
          timeout_seconds: 300
          location: your-gcp-location
          priority: interactive
          retries: 1
```
Replace 'your-gcp-project-id', 'your_dataset_name' and 'your-gcp-location' corresponding your project.

3. **Install Project Dependencies**:
Navigate to your project directory and install the required dbt packages:
    ```bash
    dbt deps
    ```
## Usage

1. **Run Models**:
    To run the dbt models and create tables/views in BigQuery:
    ```bash
    dbt run
    ```

2. **Run Tests**:
    To execute tests defined in your dbt project to ensure data integrity:
    ```bash
    dbt test
    ```

3. **Generate and View Documentation**:
    To generate and serve the documentation for your dbt project:
    ```bash
    dbt docs generate
    dbt docs serve
    ```
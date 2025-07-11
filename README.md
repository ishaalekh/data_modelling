# Azure End-to-End Data Engineering Project

This project demonstrates a complete Azure-based data engineering solution using a dataset sourced from GitHub and processed through modern cloud components, including Azure Data Factory, Azure SQL Database, ADLS Gen2, Azure Databricks, Unity Catalog, and Power BI.

## Project Overview

- **Source Data**: GitHub dataset imported into **Azure SQL Database** (<a href='https://github.com/anshlambagit/Azure-DE-Project-Resources/tree/main/Raw%20Data'>Link</a>)
- **Ingestion**: Azure Data Factory pipeline with **parameterized incremental loading** using a **watermark table**
- **Watermarking**: Managed via a **stored procedure** to update the last successful load timestamp and a water mark table with last_load_date details
- **Landing Zone**: Data stored in **Bronze container** in **ADLS Gen2**

## Data Transformation & Modeling

- **Unity Catalog Setup**: Created metastore, catalog, schemas, and external tables for Bronze, Silver, and Gold layers
- **Azure Databricks**:
  - Bronze to Silver transformations using PySpark
  - Created **dimension tables** across 4 notebooks and also implemented incremental loading in it by using UPSERT.
  - Built **fact table** and implemented a **star schema**
  - Connected all notebooks in a **Databricks Workflow job**

## Data Visualization

- Connected the **Gold layer** to **Power BI**
- Developed visual reports to analyze sales trends and performance metrics

## Technologies Used
- Azure Data Factory
- Azure SQL Database
- Azure Data Lake Storage Gen2
- Azure Databricks (Unity Catalog + Workflows)
- PySpark
- Power BI

## Key Features

- Incremental ETL using a parameterized ADF pipeline with a watermark table
- Stored procedure-driven control for last load tracking
- Modular PySpark notebooks for Silver, Dimension, and Fact transformations
- Star schema modeling for analytics-ready datasets
- Full pipeline orchestration using Databricks Workflows
- End-to-end connectivity with Power BI for visualization
- **Tested the entire pipeline by uploading a new incremental sales file and validating that only new data was ingested, transformed, and reflected accurately in downstream reports**

---

## Outcome

A fully automated, scalable, and test-validated Azure data engineering solution following best practices in ingestion, transformation, dimensional modeling, orchestration, and reporting.



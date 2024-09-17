# Fintech Data Migration Project 

![fintech!](FintechDataMigrationPipeline.png)

---

# **Fintech Data Lakehouse Architecture Project**

## **Project Overview**

---

In this project, I conceptualized the migration of data from a relational SQL Server database to a cloud-based Lakehouse architecture. The project involves the process of moving financial data from a traditional SQL database to Azure Data Lake Storage (ADLS) in a structured and scalable manner, leveraging the **Bronze**, **Silver**, and **Gold** layer approach for data storage and transformation.

The SQL Server database schema is named **Fintech**, containing the following five tables:
- **Customers**
- **Accounts**
- **Loans**
- **Payments**
- **Transactions**
---
The focus of the project is to demonstrate the migration of data into a Lakehouse architecture, applying data transformations and automating workflows using Azure Synapse Pipelines. The architecture follows a **Bronze**, **Silver**, and **Gold** layer approach:
- **Bronze Layer**: Stores raw data as it is from the source.
- **Silver Layer**: Contains data after quality checks and transformations.
- **Gold Layer**: Holds the final processed data, which can be used for analytics, reporting, or ad-hoc querying.
---
Additionally, I built a Synapse pipeline to automate the extraction, loading, and transformation (ELT) processes involved in this architecture. The data was stored in **Azure Data Lake Storage (ADLS)**.

## **Steps Involved in the Project**

### **Step 1: SQL Server to Bronze Layer Migration**

The first step was to migrate data from the SQL Server into the **Bronze Layer** in ADLS. Initially, each table (Customers, Accounts, Loans, Payments, and Transactions) was extracted and copied separately from SQL Server to ADLS using **copy activities**. However, this approach was inefficient due to the manual creation of copy activities for each table.

To automate the process, I leveraged:
- **Lookup Activity**: This activity was used to dynamically retrieve the list of tables (Customers, Accounts, Loans, Payments, and Transactions) from the **Fintech** schema in the SQL database.
- **ForEach Activity**: This loop iterated through the list of tables from the Lookup activity and executed the copy activity for each table. This eliminated the need for hardcoding individual copy activities for each table.
- **Copy Activity**: Inside the ForEach loop, I used a dynamic copy activity that fetched the data from each table and copied it into the **Bronze Layer** in ADLS.

The data was stored in ADLS under the **Fintech** container.

**Key Components in this Step:**
- **SQL Server**: Source database containing the Fintech schema and tables.
- **Azure Data Lake Storage (ADLS)**: Storage for the Bronze Layer, holding raw data.
- **Synapse Pipeline**: A pipeline to automate the movement of data from SQL Server to ADLS.
- **Copy Activity**: Used for transferring data from SQL tables to ADLS.
- **ForEach Activity**: Enabled dynamic data loading for each table without manually creating separate copy activities.

### **Step 2: Moving Data from Bronze to Silver Layer**

Once the raw data was stored in the Bronze Layer, I performed quality checks and transformations before moving it to the **Silver Layer**. This transformation process was done using **notebooks**:

- **Notebook 1**: This notebook was responsible for extracting the raw data from the Bronze Layer, performing necessary data validation, cleaning, and transformation, and then writing the processed data into the **Silver Layer**.

The Silver Layer now contained cleaned and transformed data, ready for further processing or reporting.

### **Step 3: Moving Data from Silver to Gold Layer**

After transforming the data in the Silver Layer, I further processed it and aggregated it into the **Gold Layer**. The Gold Layer typically holds fact and dimension tables, which can be used for analytical purposes.

- **Notebook 2**: This notebook extracted data from the Silver Layer, applied any additional transformations or aggregations, and wrote the final data into the **Gold Layer**.

The Gold Layer is the final, cleaned, and transformed data that is ready for reporting, analytics, and querying.

### **Step 4: Notification Setup with Logic App**

I implemented email notifications to alert on the success or failure of the pipeline execution. This was achieved using **Azure Logic App**, which sent notifications to the relevant stakeholders after the data was successfully moved to the Gold Layer or if any errors occurred during the process.

### **Step 5: Synapse Pipelines Overview**

The Synapse pipeline orchestrated the entire process:
1. **SQL Server to Bronze Layer (Copy)**: Using Lookup and ForEach activities to dynamically move data from SQL Server to ADLS in the Bronze Layer.
2. **Bronze to Silver Layer (Notebook 1)**: Running a notebook to clean and transform the data in the Silver Layer.
3. **Silver to Gold Layer (Notebook 2)**: Running another notebook to perform final transformations and aggregations in the Gold Layer.
4. **Email Notifications**: Using Logic App to notify upon pipeline success or failure.
## Pipeline Running in Azure
---
![pipeline!](Pipeline_On_The_Azure_Synapse.jpg)
### **Step 6: Data Storage in Azure Data Lake (ADLS)**

In ADLS, I created a container called **Fintech**, In this cointainer I have created three folders:
1. **Bronze Layer**: Contains raw data from the SQL Server tables.
2. **Silver Layer**: Holds data after transformation and quality checks.
3. **Gold Layer**: Contains aggregated and transformed data, ready for analytics.

## **Key Benefits of this Architecture**

- **Automation**: The use of Synapse’s dynamic activities (Lookup, ForEach) allowed me to automate the movement of multiple tables without manually configuring separate pipelines for each.
- **Scalability**: The Lakehouse architecture can easily accommodate more tables in the future by adjusting the pipeline.
- **Data Quality**: Data was validated and cleaned in the Silver Layer, ensuring that only high-quality data reached the final Gold Layer.
- **Agility**: The separation of raw, cleaned, and transformed data into different layers (Bronze, Silver, and Gold) allows for flexible reporting, analytics, and further processing.

## **Challenges Encountered**

- **Initial Manual Configuration**: The initial attempt to create individual copy activities for each table was inefficient and repetitive. I resolved this by using dynamic Lookup and ForEach activities in Synapse.
- **Error Handling**: Handling errors during the pipeline execution, such as connection issues with SQL Server,ADLS and Azure Synapse.

## **Conclusion**

This project successfully implemented a Lakehouse architecture for a fintech use case. By automating the data migration process with Synapse Pipelines and organizing data into Bronze, Silver, and Gold layers in ADLS, I created a scalable, efficient, and agile architecture. This Lakehouse design can support future analytics and reporting needs, with Synapse and Logic Apps providing a robust orchestration and notification mechanism.

---




### **End-to-End Data Engineering pipeline On Fintech Data Migration and Lakehouse Architecture Project**

### **Project Overview**

This project demonstrates the migration of data from a relational SQL Server database to a cloud-based Lakehouse architecture. The migration process involves moving financial data from a traditional SQL database to Azure Data Lake Storage (ADLS) in a structured and scalable manner, using the **Bronze**, **Silver**, and **Gold** layer approach for data storage and transformation.

The SQL Server database schema is named **Fintech**, containing the following five tables:
- **Customers**
- **Accounts**
- **Loans**
- **Payments**
- **Transactions**

The project focuses on the migration of data into a Lakehouse architecture, applying data transformations and automating workflows using Azure Synapse Pipelines. The architecture follows a **Bronze**, **Silver**, and **Gold** layer approach:
- **Bronze Layer**: Stores raw data as it is from the source.
- **Silver Layer**: Contains data after quality checks and transformations.
- **Gold Layer**: Holds the final processed data, which can be used for analytics, reporting, or ad-hoc querying.

In addition, a Synapse pipeline was built to automate the extraction, loading, and transformation (ELT) processes involved in this architecture. After building the pipeline in Synapse, data from the **Gold Layer** was stored in **Azure Data Lake Storage (ADLS)** for further use.

### **Architecture**
![Architecture!](FintechDataMigrationPipeline.png)

### **Synapse Pipelines Overview**

The Synapse pipeline orchestrated the entire process:
1. **SQL Server to Bronze Layer (Copy)**: Using Lookup and ForEach activities to dynamically move data from SQL Server to ADLS in the Bronze Layer.
2. **Bronze to Silver Layer (Notebook 1)**: Running a notebook to clean and transform the data in the Silver Layer.
3. **Silver to Gold Layer (Notebook 2)**: Running another notebook to perform final transformations and aggregations in the Gold Layer.
4. **Email Notifications**: Using Logic App to notify upon pipeline success or failure.

### **Steps Involved in the Project**

### **Step 1: SQL Server to Bronze Layer**

The first step was migrating data from the SQL Server into the **Bronze Layer** in ADLS. Initially, each table (Customers, Accounts, Loans, Payments, and Transactions) was extracted and copied separately from SQL Server to ADLS using **copy activities**. However, this approach was inefficient due to the manual creation of copy activities for each table.

To automate the process:
- **Lookup Activity** was used to dynamically retrieve the list of tables (Customers, Accounts, Loans, Payments, and Transactions) from the **Fintech** schema in the SQL database.
- **ForEach Activity** was used to loop through the list of tables from the Lookup activity and execute the copy activity for each table. This eliminated the need for hardcoding individual copy activities for each table.
- **Copy Activity** inside the ForEach loop dynamically fetched data from each table and copied it into the **Bronze Layer** in ADLS.

Data was stored in ADLS under the **Fintech** container.

**Key Components in this Step:**
- **SQL Server**: Source database containing the Fintech schema and tables.
- **Azure Data Lake Storage (ADLS)**: Storage for the Bronze Layer, holding raw data.
- **Synapse Pipeline**: Pipeline to automate data movement from SQL Server to ADLS.
- **Copy Activity**: Transfers data from SQL tables to ADLS.
- **ForEach Activity**: Enables dynamic data loading for each table without manually creating separate copy activities.

### **Step 2: Moving Data from Bronze to Silver Layer**

Once raw data was stored in the Bronze Layer, quality checks and transformations were performed before moving it to the **Silver Layer**. This transformation process was carried out using **notebooks**.

- **Notebook 1**: Extracted raw data from the Bronze Layer, performed data validation, cleaning, and transformation, and wrote the processed data into the **Silver Layer**.

The Silver Layer now contains cleaned and transformed data, ready for further processing.

### **Step 3: Moving Data from Silver to Gold Layer**

After the data in the Silver Layer was transformed, it was further processed and aggregated into the **Gold Layer**, which typically holds fact and dimension tables for analytical purposes.

- **Notebook 2**: Extracted data from the Silver Layer, applied any additional transformations or aggregations, and wrote the final data into the **Gold Layer**.

The **Gold Layer** contains the final, cleaned, and transformed data, making it ready for reporting, analytics, and querying.

### **Step 4: Notification Setup with Logic App**

Email notifications were set up to alert stakeholders on the success or failure of the pipeline execution. This was achieved using **Azure Logic App**, which sent notifications after the data was successfully moved to the Gold Layer or if any errors occurred during the process.
![data pipeline!](Pipeline_On_The_Azure_Synapse.jpg)
### **Step 5: Data Storage in Azure Data Lake (ADLS)**

In ADLS, a container named **Fintech** was created. Inside this container, three folders were organized:
1. **Bronze Layer**: Contains raw data from SQL Server tables.
2. **Silver Layer**: Holds data after transformation and quality checks.
3. **Gold Layer**: Contains aggregated and transformed data, ready for analytics.

### **Key Benefits of this Architecture**

- **Automation**: Synapse’s dynamic activities (Lookup, ForEach) automate the movement of multiple tables without manually configuring separate pipelines for each.
- **Scalability**: The Lakehouse architecture can easily accommodate more tables in the future by adjusting the pipeline.
- **Data Quality**: Data is validated and cleaned in the Silver Layer, ensuring only high-quality data reaches the final Gold Layer.
- **Agility**: The separation of raw, cleaned, and transformed data into different layers (Bronze, Silver, and Gold) allows for flexible reporting, analytics, and further processing.

### **Challenges Encountered**

- **Initial Manual Configuration**: Creating individual copy activities for each table was inefficient and repetitive, resolved by using dynamic Lookup and ForEach activities in Synapse.
- **Error Handling**: Issues with connection to SQL Server, ADLS, and Azure Synapse required attention during the pipeline execution.

### **Scripts and data for this Project**
1. [Spark-notebook](spark-notebook/BronzeToSilverDataProcess.ipynb)
2. [Spark-notebook](spark-notebook/SilverToGoldDataProcess.ipynb)
3. [Sql-Database-Table](Sql-Database-Table/Accounts.sql)
4. [Sql-Database-Table](Sql-Database-Table/Customers.sql)
5. [Sql-Database-Table](Sql-Database-Table/Loans.sql)
6. [Sql-Database-Table](Sql-Database-Table/Payments.sql)
7. [Sql-Database-Table](Sql-Database-Table/Transactions.sql)
### **Conclusion**

The project successfully implemented a Lakehouse architecture for fintech data migration. By automating the data migration process with Synapse Pipelines and organizing data into Bronze, Silver, and Gold layers in ADLS, a scalable, efficient, and agile architecture was created. This design supports future analytics and reporting needs, with Synapse and Logic Apps providing a robust orchestration and notification mechanism.




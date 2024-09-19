
### **End-to-End Data Engineering pipeline On Fintech Data Migration and Lakehouse Architecture Project**

### **Project Overview**
Yes, there is some repetitive information regarding the **Bronze**, **Silver**, and **Gold** layer approach and the **Synapse pipeline**. Here's a more concise version of your description, removing redundancies:

This project demonstrates the migration of data from a **relational SQL Server database** to a **cloud-based Lakehouse architecture**. The migration process involves moving financial data from a traditional SQL database to **Azure Data Lake Storage (ADLS)** using a structured and scalable approach.

The SQL Server database schema, named **Fintech**, contains the following five tables:
- Customers
- Accounts
- Loans
- Payments
- Transactions

The data migration and transformation process is organized using the **Bronze, Silver, and Gold** layer architecture:
- **Bronze Layer**: Raw data as extracted from the source.
- **Silver Layer**: Data after quality checks and transformations.
- **Gold Layer**: Fully processed data, ready for analytics and reporting.

A Synapse pipeline was created to automate the **extraction, loading, and transformation (ELT)** processes, ensuring seamless data flow through these layers. After transformations, the final data from the **Gold Layer** is stored in **Azure Data Lake Storage (ADLS)** for further use.
### **Architecture**
![Architecture!](FintechDataMigrationPipeline.png)

### **Synapse Pipelines Overview**

The Synapse pipeline orchestrated the entire data migration and transformation process:

1. **SQL Server to Bronze Layer (Copy)**: Data from SQL Server is dynamically copied to ADLS using a combination of **Lookup** and **ForEach** activities, which automate the transfer of tables (Customers, Accounts, Loans, Payments, and Transactions) to the Bronze Layer.
2. **Bronze to Silver Layer (Notebook 1)**: A notebook is executed to clean and transform raw data in the Bronze Layer and store the processed data in the Silver Layer.
3. **Silver to Gold Layer (Notebook 2)**: Another notebook runs to apply final transformations and aggregations on Silver Layer data, storing the final processed data in the Gold Layer.
4. **Email Notifications**: A **Logic App** sends email notifications to stakeholders regarding pipeline success or failure.

![Pipeline!](Pipeline_On_The_Azure_Synapse.jpg)
### **Steps Involved in the Project**

**Step 1: SQL Server to Bronze Layer**  
The first step involved migrating tables from the SQL Server into the Bronze Layer in ADLS. Initially, separate copy activities were created for each table, but this approach was automated using **Lookup** and **ForEach** activities, which dynamically fetch the list of tables and loop through each to execute the copy activity, eliminating manual work.
### **Technology Used**

1. Programming Language - Python  
2. Scripting Language - SQL 
3. Pyspark
4. Azure Cloud Platform
    - Azure ADLS
    - Azure SQL Database
    - Delta table
    - Azure Synapse Ayalytics
Key Components:
- **SQL Server**: Source database.
- **Azure Data Lake Storage (ADLS)**: Storage for raw data in the Bronze Layer.
- **Synapse Pipeline**: Used to automate data movement from SQL Server to ADLS.

**Step 2: Moving Data from Bronze to Silver Layer**  
Once in the Bronze Layer, data underwent validation and transformation using **Notebook 1**. The transformed data was stored in the Silver Layer, where it is cleaned and ready for further processing.

**Step 3: Moving Data from Silver to Gold Layer**  
**Notebook 2** handled further transformations and aggregations on the Silver Layer data, moving the final processed data to the Gold Layer, which is now ready for analytics, reporting, and querying.

**Step 4: Notification Setup with Logic App**  
Azure **Logic App** was configured to send email notifications upon pipeline success or failure, ensuring stakeholders are informed about the pipeline execution.

**Step 5: Data Storage in Azure Data Lake (ADLS)**  
Data was stored in the **Fintech** container in ADLS, organized into three layers:
- **Bronze**: Raw data.
- **Silver**: Cleaned and transformed data.
- **Gold**: Final aggregated and processed data.

### **Key Benefits of this Architecture**
- **Automation**: Dynamic activities such as **Lookup** and **ForEach** automate the process, eliminating the need for manually configuring pipelines for each table.
- **Scalability**: The architecture can easily accommodate additional tables or data sources in the future.
- **Data Quality**: Silver Layer ensures data validation and cleaning, ensuring only high-quality data reaches the Gold Layer.
- **Agility**: The separation into Bronze, Silver, and Gold layers allows for flexible data processing and analytics.

### **Challenges Encountered**
- **Initial Manual Configuration**: Creating individual copy activities for each table was inefficient, later resolved by using dynamic activities.
- **Error Handling**: Connection issues with SQL Server, ADLS, and Synapse required troubleshooting during the pipeline execution.


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




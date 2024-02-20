This project provides a data engineering journey on the Movie Recommendation dataset, the data is ingested into the Azure ecosystem via Azure Data Factory. 
It's initially stored in Azure Data Lake Storage Gen2, then transformed in Azure Databricks.

Azure Services Used

Azure Data Factory: For data ingestion from TSE website.
Azure Data Lake Storage Gen2: As the primary data storage solution.
Azure Databricks: For data transformation tasks.
Azure Key Vault: For security of secret key value.

Workflow

Initial Setup in Azure

1. Create Azure account (Free Subscription)
2. Create a Resource Group 'MovieRecommanded' to house and manage all the Azure resources associated with this project.
3. Within the created resource group, setup a storage account. This is specifically configured to leverage Azure Data Lake Storage(ADLS) Gen2 capabilities.
4. Create a Container inside this storage account to hold the project's data. Two directories 'raw-data' ,rejected & 'validated-data' are created to store raw data and transformed data.

Data Ingestion using Azure Data Factory

1. First, created an Azure Data Factory workspace within the previously established resource group (MovieRecommanded).
2. After setting up the workspace, launch the Azure Data Factory Studio.
3. Upload the Election-Candidates dataset from TSE portal.
4. Within the studio, initialize a new data integration pipeline.

Data Transformation using Azure Databricks
1. Navigate to Azure Databricks within the Azure portal and create a workspace within the previously established resource group and launch it.
2. Configuring Compute in Databricks
3. Create a new notebook within Databricks and rename it appropriately, reflecting its purpose or the dataset it pertains to.
4. Establishing a Connection to Azure Data Lake Storage (ADLS) using App Registration.
  Create a new registration;
  Copy the credentials (Client ID, Tenant ID), to later write the appropriate code in the Databricks notebook to mount ADLS.
  In Certificate & Secrets: create a secret for later on store in in the Key Vault, for security purposes.

5. Using Key Vault in the Azure account for Secure key protecting:
  In Key Vault, create a key vault with the same resource used before;
  In the IAM add roles: Secret Officer with the member as your User and Secrets User with the member as 'AzureDatabricks'.
  Now in the 'Secrets', generate a secret with the App Registration Key secret, name it and create.
  This Key Vault will secure your secret key so it exposed in the mount code.

6. In Databricks, created a secret scope:
  To create it, substitute go to https://<databricks_instance.net>#secrets/createScope;
  Create a Scope using the DNS and Resource ID from the Key Vault properties.
7. Writing Data Transformations mount ADLS Gen2 to Databricks.
  Within the code using python with OS library, removed files that were not used. (code is provided in the reference below)
  Create a dataframe (pyspark), transformed columns with the correct datatype and created table within a database to query.

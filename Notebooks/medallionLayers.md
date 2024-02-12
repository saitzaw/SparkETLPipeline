## Three layers for Bronze, Silver and Gold
- make the delta table to save data
- make the s3 path for Delta table [it isn't to do with AWS, Databricks will handle]
- make the difference layers of storages to get the data security
## Bronze layer 
  - Staging layer for the data [landing zone]
## Silver layer 
  - Data storage zone
  - Data quality control and ckeck the data before sending to gold layer
  - Quarantine the data 
## Gold layer 
  - store layer
  - Materialized view 

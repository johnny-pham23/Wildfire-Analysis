# Wildfire Analysis
**For the completion of Big Data Management**  
# Goals
- Apply in class techniques on a real big-data application.
- Translate high-level requirements into a big-data processing pipeline.
- Break down a big problem into smaller parts that can be solved separately.

# Overview
This project analyzes a dataset the represents the wildfire occurrences in the US. The dataset contains a total of 18 million points. However, we analyzed with only a subset of the data in California. Furthermore, several subsets of the data of sizes, 1K, 10K, and 100K, will be tested on to expedite the development.

The hourly power consumption data comes from PJM's website and are in megawatts (MW).  
*The regions have changed over the years so data may only appear for certain dates per region.*  
# Data Structure

Datetime | zone_load_MW | tmpf | hour | month | dayofweek  
-------- | -------------| ---- | ---- | ----- | --------
2004-10-02 00:00:00 | 13147.0 | 69.08 | 0 | 10 | 2 




## To Do
- Split dataset to test/train
- Build model
- Validate model

## Completed 
- Check null/missing values in load profile.
- Merge load data to temperature data using datetime as key.
- Drop null values created from merge

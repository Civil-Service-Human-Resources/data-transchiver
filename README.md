### Guide - CSL data transchiver process

#### What the process does?
This is a custom built node application that does the below
1. Identify the candidate records (statements) from source (Cosmos DB) for archiving
2. Move the identfied records to the target store (Mysql)

***Note*** 
*Move above does the copy first to the target and then deltes the same from the source following the successful copy operation.*

#### Dependencies

You should have the following installed in the environment you're planning to test

##### Core binaries

- NodeJS and NPM
- Mysql server

##### Core libraries

- express
- mongodb
- mysql
- node-cron
- applicationinsights (not integrated yet)

#### How to run the node app

1. Download the code and switch to the folder `data-transchiver`
2. Run `npm install`
3. export the required environment variables as mentioned below
4. Create the target Mysql database and the statements_history table
```
CREATE DATABASE IF NOT EXISTS db_archiver;
USE db_archiver;
CREATE TABLE IF NOT EXISTS db_archiver.statements_history(
	statement_hash varchar(50) primary key,
	user_id varchar(50),
	inserted_date DATETIME,
	statement_date DATETIME,
    statement JSON
) ENGINE=InnoDB;
CREATE INDEX idx_user_id ON statements_history(user_id);
CREATE INDEX idx_inserted_date ON statements_history(inserted_date);
CREATE INDEX idx_statement_date ON statements_history(statement_date);
```
4. Run the app as below `node app.js`

#### Environment variables

##### This is for the process's meta data
```
TDS_MYSQL_PROC_REGISTRY_HOST
TDS_MYSQL_PROC_REGISTRY_DB_USER
TDS_MYSQL_PROC_REGISTRY_DB_PWD
```
##### This is of the CSL learner recrods data
```
TDS_MYSQL_LEARNER_RECORD_DB_HOST
TDS_MYSQL_LEARNER_RECORD_DB_USER
TDS_MYSQL_LEARNER_RECORD_DB_PWD
```
##### This is for the target history records (statements)
```
TDS_MYSQL_HISTORY_DB_HOST
TDS_MYSQL_HISTORY_DB_USER
TDS_MYSQL_HISTORY_DB_PWD
```
###### This is of the Cosmos (MongoDB) source 
```
COSMOS_SRC_CONNECTION_STRING
```
###### This is the schedule (cron like scheduler) for the process to kickoff. 
```
DATA_XFR_JOB_SCHEDULE
```
###### Optional environment variables

delete batch size for statements. 100 is a recommended number and also the default.
```
STATEMENTS_DELETE_BATCH_SIZE
```

Runtime (cutt-off time) for the job. The default runtime has been set to 9 hrs (32400 seconds). When supplied will stop the job and terminate the node process. The runtime is only effective when the task job has started.
```
TDS_RUNTIME_IN_SECONDS
```
------------


#### Ops Guide for support staff

All sequel queries mentioned below should be run against the Mysql server that is setup for the transitory store.

The process creates three tables in the mysql database, and they are:

| #   |  Table Name |
| ------------ | ------------ |
| 1  | task_registry  |
| 2  | candidate_record  |
| 3  | statements_history  |


The process updates the first two tables to hold the meta information while executing, and the data that is copied from the source (Cosmos MongoDB) will be stored (transferred) into the 3rd table (statements_history).

------------
##### FAQs

###### Question (1)
>How do I check the progress / status of the process?

###### Answer
>Run the below sequel statement against the target mysql server

```
SELECT * FROM db_archiver.tasks_registry;
```

_Example output_

|  id | name  | start_time  | elapsed_seconds  |  status |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| 1 |  DataIdentifier | 2019-11-29 15:50:00	  |  NULL |  RUNNING |
| 2  |  DataTransfer | NULL  | NULL  | NOT_READY  |

_The possible values in the status column are NOT_READY, READY, RUNNING and COMPLETED. So, when both tasks have completed you will see the status column of those tasks as below_

_Example output_

|  id | name  | start_time  | elapsed_seconds  |  status |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| 1 |  DataIdentifier | 2019-11-29 15:50:00	  |  708.580647353 |  COMPLETED |
| 2  |  DataTransfer | 2019-11-29 16:02:00  | 25939.629658938  | COMPLETED  |


In addition, the process will also print some summary to the console so if you have access to the platform you should an output similar to below.

_Example output_

| DataTransfer task is running.... | |
| ------------ | ------------ |
| learner records found | 100 |
| statements found  | 20000 |
| statements copied  | 20000 |
| statements replaced | 0 |
| statements deleted   | 20000 |
| transfer status    | Success |

The process' transfer status will reflect “**Failed**” when the process has failed.

------------

###### Question (2)
>How do I know if there were any errors during the transfer process?

###### Answer
>Run the below query, and you will know when the ERROR count is greater than zero

```
SELECT COUNT(*) Errors 
FROM db_archiver.candidate_record 
WHERE copy_status = 'ERROR' or delete_status = 'ERROR';
```

Example output

|  Errors |
| ------------ |
| 0 |

------------

###### Question (3)
>How do I know the number of statements copied vs. deleted?

###### Answer
>Run the below query, and you will know when the ERROR count is greater than zero

```
SELECT SUM(deleted_count) deleted_total, SUM(copied_count) copied_total 
FROM db_archiver.candidate_record;
```

Example output

| deleted_total  | copied_total  |
| ------------ | ------------ |
| 2,500,356  | 2,500,356   |


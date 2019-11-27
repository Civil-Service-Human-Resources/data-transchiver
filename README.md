## Test Guide (WIP)

### Brief description of the application process

### Dependencies

You should have the following installed in the environment you're planning to test

* NodeJS and NPM
* Mysql server

### Inputs to the process

### Error handling and reporting

### How to run the node app

1. Download the code and switch to the folder ```data-transchiver```

2. Run ```npm install```

3. export the required environment variables locally as mentioned below

Example

``` ENV TDS_MYSQL_PROC_REGISTRY_HOST```
``` ENV TDS_MYSQL_PROC_REGISTRY_DB_USER```
``` ENV TDS_MYSQL_PROC_REGISTRY_DB_PWD```

``` ENV TDS_MYSQL_LEARNER_RECORD_DB_HOST```
``` ENV TDS_MYSQL_LEARNER_RECORD_DB_USER```
``` ENV TDS_MYSQL_LEARNER_RECORD_DB_PWD```
    
``` ENV TDS_MYSQL_HISTORY_DB_HOST```
``` ENV TDS_MYSQL_HISTORY_DB_USER```
``` ENV TDS_MYSQL_HISTORY_DB_PWD```

``` ENV COSMOS_SRC_CONNECTION_STRING```
``` ENV DATA_XFR_JOB_SCHEDULE```

4. run the app as below

```node app.js```


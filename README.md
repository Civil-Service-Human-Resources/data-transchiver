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

```export MYSQL_SERVER_HOST=<hostname>```
```export MYSQL_DB_USER=<mysql_user>```
```export MYSQL_DB_PWD=<mysql_user_password>```
```export DATA_XFR_JOB_SCHEDULE=<cron_like_schedule>```

Example:

```export MYSQL_SERVER_HOST=localhost```
```export MYSQL_DB_USER=root```
```export MYSQL_DB_PWD=password```
```export DATA_XFR_JOB_SCHEDULE="5 * * * * *"``` 

4. run the app as below

```node app.js```


const appInsights = require('applicationinsights');
const scheduler = require('./scheduler.js');
const tasks = require('./tasks-wrapper.js');

const app = require('express')();
const APP_HTTP_PORT = 8080;

const APP_NAME = require(__dirname + '/package.json').name

let jobs =  [];

let loadEnvVariables = () => {
  let requiredEnv = [
    'TDS_MYSQL_PROC_REGISTRY_HOST',
    'TDS_MYSQL_PROC_REGISTRY_DB_USER',
    'TDS_MYSQL_PROC_REGISTRY_DB_PWD',

    'TDS_MYSQL_LEARNER_RECORD_DB_HOST',
    'TDS_MYSQL_LEARNER_RECORD_DB_USER',
    'TDS_MYSQL_LEARNER_RECORD_DB_PWD',
    
    'TDS_MYSQL_HISTORY_DB_HOST',
    'TDS_MYSQL_HISTORY_DB_USER',
    'TDS_MYSQL_HISTORY_DB_PWD',

    'COSMOS_SRC_CONNECTION_STRING',
    'DATA_XFR_JOB_SCHEDULE',
  ];

  let unsetEnv = requiredEnv.filter((env) => !(typeof process.env[env] !== 'undefined'));

  if (unsetEnv.length > 0) {
    throw new Error("Rrequired ENVIRONMENT variables are not set: [" + unsetEnv.join(', ') + "]");
  }
}

let callback = (taskName) => {
  jobs.forEach(job => {
    
    if ( job.id === taskName){
      scheduler.stopSchedule(job.task);

      if (job.id === "DataIdentifier"){
        let _schedule = scheduler.getSchedule(true);
        let job = scheduler.createSchedule(_schedule, function(){
          tasks.DataTransfer(_schedule, callback);
        });  
        jobs.push({"id": "DataTransfer", "task": job});
      }
    }
  });
}

let startSchedule = () => {
  tasks.prepare("DataIdentifier");

  let _schedule = scheduler.getSchedule();
  let job = scheduler.createSchedule(_schedule, function(){
    tasks.DataIdentifier(_schedule, callback);
  });
  
  jobs.push({"id": "DataIdentifier", "task": job});
}

app.get('/', (req, res) => {
  res.status(200).json({"message": APP_NAME + " said OK"});
});

app.listen(APP_HTTP_PORT, () => console.log(
  APP_NAME + " is listening on port " + APP_HTTP_PORT)
);
loadEnvVariables()
startSchedule();





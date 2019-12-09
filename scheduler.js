const cron = require('node-cron');
const CircularJSON = require('circular-json');    
const sprintf=require("sprintf-js").sprintf;
const tasks = require('./tasks-wrapper.js');

/* Scheduler parameters - read from environment variable below
* DATA_XFR_JOB_SCHEDULE
# ┌────────────── second (optional)
# │ ┌──────────── minute
# │ │ ┌────────── hour
# │ │ │ ┌──────── day of month
# │ │ │ │ ┌────── month
# │ │ │ │ │ ┌──── day of week
# │ │ │ │ │ │
# │ │ │ │ │ │
# * * * * * *
*/
var jobs = [];
var processStartTime, timeOutTimeInSeconds = null;
var timeoutTaskHandle = null;

const DEFAULT_RUNTIME_SECONDS = 32400; // 9 hours
const JOB_RUNTIME_SECONDS = process.env.TDS_RUNTIME_IN_SECONDS || DEFAULT_RUNTIME_SECONDS;
const MILLIS_IN_SECOND = 1000;

let scheduler = {
    createSchedule: (_schedule, _task) => {
        let job = cron.schedule(_schedule, _task, {scheduled: true});
        console.info(sprintf("\nscheduled task %s",CircularJSON.stringify(job)));
        return job;
    },
    startSchedule: (_job) => {
        console.info(sprintf("\nstarting task %s",CircularJSON.stringify(_job)));
        _job.start();
    },
    stopSchedule: (_job) => {
        console.info(sprintf("\nstopping task %s",CircularJSON.stringify(_job)));
        _job.task.stop();
        _job.task.destroy();
        jobs.splice(jobs.indexOf(_job), 1);
    },
    validate: (_schedule) => {
        return cron.validate(_schedule);
    },
    timeout: async (_callback) => {
        if (jobs.length > 0){
            for (const job of jobs){
                if (null !== job && undefined !== job.task && null !== job.task){
                    if ( "destroyed" !== job.task.getStatus() ){
                        scheduler.stopSchedule(job);
                        await tasks.updateStatus(job.id, "INTERRUPTED");
                        console.info("Stopped task " + job.id);
                    }
                    job.task = null;
                }
            }
            timeoutTaskHandle.stop();
            timeoutTaskHandle.destroy();
            timeoutTaskHandle = null;
            jobs = [];
            _callback();
        }
    },
    updateTasksList: (_jobref) => {
        jobs.push(_jobref);
    },
    runTimeOutTask: async (_callback) => {
        if (null === timeoutTaskHandle) {
            const now = new Date();  
            const future = new Date(now.getTime() + JOB_RUNTIME_SECONDS * MILLIS_IN_SECOND);
    
            processStartTime = Math.round(now.getTime() / MILLIS_IN_SECOND);
            timeOutTimeInSeconds = Math.round(future.getTime() / MILLIS_IN_SECOND);
    
            console.info("\nstartTime : " + processStartTime + ", cuttOffTime : " + timeOutTimeInSeconds);
    
            timeoutTaskHandle = scheduler.createSchedule("*/10 * * * * *", function(){
                scheduler.timeOutChecker(_callback);
            });   
        }
    }, 
    timeOutChecker: (_callback) => {

        const now = new Date();
        const currentTimeInSeconds = Math.round(now.getTime() / 1000);

        if ( currentTimeInSeconds >= timeOutTimeInSeconds){
            console.info("\nTimeout event fired. requesting scheduler to stop the tasks....")
            scheduler.timeout(_callback);
        }
    },
    getSchedule: (immediate=false) => {
        let schedule = null;
        const delay_ms = 10000;

        if (immediate){
            const timeNow =  new Date(Date.now() + delay_ms);
            const second = timeNow.getSeconds();
            const minute = timeNow.getMinutes();
            const hour = timeNow.getHours();
            
            const month = timeNow.getMonth();
            const day = timeNow.getDate();
            const weekday = timeNow.getDay();
            
            schedule = second + " " + minute + " " + hour + " " + day + " " + (month+1) + " " + (weekday+1);    
        }else{
            schedule = process.env.DATA_XFR_JOB_SCHEDULE;
        }
        if (!scheduler.validate(schedule)){
            throw "Error: Not a valid schedule. Aborting..";
        }
        return schedule;
    }
}

module.exports = scheduler;





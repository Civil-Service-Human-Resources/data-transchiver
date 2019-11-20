let cron = require('node-cron');
let CircularJSON = require('circular-json');    
let sprintf=require("sprintf-js").sprintf;

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

let scheduler = {
    createSchedule: (_schedule, _task) => {
        let job = cron.schedule(_schedule, _task, {scheduled: true});
        console.log(sprintf("\nscheduled task %s",CircularJSON.stringify(job)));
        return job;
    },
    startSchedule: (_job) => {
        console.log(sprintf("\nstarting task %s",CircularJSON.stringify(_job)));
        _job.start();
    },
    stopSchedule: (_job) => {
        console.log(sprintf("\nstopping task %s",CircularJSON.stringify(_job)));
        _job.stop();
        _job.destroy();
    },
    validate: (_schedule) => {
        return cron.validate(_schedule);
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





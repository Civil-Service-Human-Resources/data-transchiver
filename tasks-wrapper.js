const db = require('./db/dbService.js');
const dataIdentifier = require('./tasks/data-identifier.js');
const dataTransfer = require('./tasks/data-transfer.js');
const moment = require('moment');

const _tasks = [
    [1, 'DataIdentifier', 'NOT_READY'],
    [2, 'DataTransfer', 'NOT_READY']
];

const updateTasksTable     = "UPDATE db_archiver.tasks_registry SET ";
const selectFromTasksTable = "SELECT name, status from db_archiver.tasks_registry ";

const Tasks = {
    printSchedule: (_schedule) => {
        console.info("\n\ntask scheduled at " + _schedule + " and running at " + new Date());
    },
    reset: async () => {
        await db.createSchema();
        await db.insertRecords(_tasks);
        await Tasks.queryAll();
    },
    prepare: async (_taskName) => {
        // db.printConfigs();
        await db.copyFromLastRunToNewTable();
        await Tasks.reset();
        await Tasks.updateStatus(_taskName, 'READY');
    },
    queryAll: async () => {
        let results = await db.queryRecords(
            selectFromTasksTable
        );
        console.table([results[0], results[1]]);
    },
    queryStatus: async (_taskName) => {
        return await db.queryRecords(
            selectFromTasksTable + "WHERE name = '" + _taskName + "'"
        );
    },
    updateStatus: async (_taskName, _status) => {
        await db.queryRecords(
            updateTasksTable + "status = '" + _status + "' WHERE name = '" + _taskName + "'"
        );
    },
    updateElapsedTime: async (_taskName, _seconds) => {
        await db.queryRecords(
            updateTasksTable + "elapsed_seconds = " + _seconds + " WHERE name = '" + _taskName + "'"
        );
    },
    updateStartTime: async (_taskName, _startedAt) => {
        await db.queryRecords(
            updateTasksTable + "start_time = '" + _startedAt + "' WHERE name = '" + _taskName + "'"
        );
    },
    getTime: () => {
        return moment().format('YYYY-MM-DD HH:mm:ss');
    },
    DataIdentifier: async (_schedule, _callback) => {
        const task_name = 'DataIdentifier';
        const dependant_task = 'DataTransfer';

        if ( await Tasks.isTaskReady(task_name) && Tasks.isTaskNotReady(dependant_task) ){
            let startTime = Tasks.getTime();
            await Tasks.updateStartTime(task_name, startTime);

            await Tasks.updateStatus(task_name, 'RUNNING');
            let timeElapsed = await dataIdentifier.execute();

            if (Tasks.isTaskRunning(task_name)){
                await Tasks.updateStatus(task_name, 'COMPLETED');
                await Tasks.updateElapsedTime(task_name, timeElapsed);
                
                if ( Tasks.isTaskNotReady(dependant_task) ){
                    await Tasks.updateStatus("DataTransfer", 'READY');
                }
            }
        }
        _callback(task_name);
    },
    DataTransfer: async (_schedule, _callback) => {
        const task_name = 'DataTransfer';
        const dependant_task = 'DataIdentifier';

        if ( await Tasks.isTaskReady(task_name) && Tasks.isTaskCompleted(dependant_task) ){
            let startTime = Tasks.getTime();
            await Tasks.updateStartTime(task_name, startTime);

            await Tasks.updateStatus(task_name, 'RUNNING');
            let timeElapsed = await dataTransfer.execute();

            if (Tasks.isTaskRunning(task_name)){
                await Tasks.updateStatus(task_name, 'COMPLETED');
                await Tasks.updateElapsedTime(task_name, timeElapsed);
            }
        }
        _callback(task_name);
    },
    isTaskCompleted: async (_taskName) => {
        let results = await Tasks.queryStatus(_taskName);
        if ( null !== results && undefined !== results[0] && 
            results[0].status === "COMPLETED" ){
            return true;
        }
        return false;
    },
    isTaskReady: async (_taskName) => {
        let results = await Tasks.queryStatus(_taskName);
        if ( null !== results && undefined !== results[0] &&
             results[0].status === "READY" ){
            return true;
        }
        return false;
    },
    isTaskNotReady: async (_taskName) => {
        let results = await Tasks.queryStatus(_taskName);
        if ( null !== results && undefined !== results[0] &&
             results[0].status === "NOT_READY" ){
            return true;
        }
        return false;
    },
    isTaskRunning: async (_taskName) => {
        let results = await Tasks.queryStatus(_taskName);
        if ( null !== results && undefined !== results[0] &&
             results[0].status === "RUNNING" ){
            return true;
        }
        return false;
    }
}

module.exports = Tasks;
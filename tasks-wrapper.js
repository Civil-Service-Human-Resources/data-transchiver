let db = require('./db/dbService.js');
let dataIdentifier = require('./tasks/data-identifier.js');
let dataTransfer = require('./tasks/data-transfer.js');
let moment = require('moment');

const _tasks = [
    [1, 'DataIdentifier', 'NOT_READY'],
    [2, 'DataTransfer', 'NOT_READY']
];

const updateTasksTable     = "UPDATE db_archiver.tasks_registry SET ";
const selectFromTasksTable = "SELECT name, status from db_archiver.tasks_registry ";

const Tasks = {
    printSchedule: (_schedule) => {
        console.log("\n\ntask scheduled at " + _schedule + " and running at " + new Date());
    },
    reset: async () => {
        await db.createSchema();
        await db.insertRecords(_tasks);
        await Tasks.queryAll();
    },
    prepare: async (_taskName) => {
        // db.printConfigs();
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

        if ( await Tasks.isTaskReady(task_name) ){
            await Tasks.updateStatus(task_name, 'RUNNING');

            await Tasks.updateStartTime(task_name, Tasks.getTime());
            let timeElapsed = await dataIdentifier.execute();

            await Tasks.updateStatus(task_name, 'COMPLETED');
            await Tasks.updateElapsedTime(task_name, timeElapsed);
            await Tasks.updateStatus("DataTransfer", 'READY');
        }
        _callback(task_name);
    },
    DataTransfer: async (_schedule, _callback) => {
        const task_name = 'DataTransfer';

        if ( await Tasks.isTaskReady(task_name) ){
            await Tasks.updateStatus(task_name, 'RUNNING');

            await Tasks.updateStartTime(task_name, Tasks.getTime());
            let timeElapsed = await dataTransfer.execute();

            await Tasks.updateStatus(task_name, 'COMPLETED');
            await Tasks.updateElapsedTime(task_name, timeElapsed);
        }
        _callback(task_name);
    },
    isTaskReady: async (_taskName) => {
        let results = await Tasks.queryStatus(_taskName);
        if ( null !== results && results[0].status === "READY" ){
            return true;
        }
        return false;
    }
}

module.exports = Tasks;
let db = require('./db/dbService.js');
let dataIdentifier = require('./tasks/data-identifier.js');
let dataTransfer = require('./tasks/data-transfer.js');
let moment = require('moment');

const _tasks = [
    [1, 'DataIdentifier', 'NOT_READY'],
    [2, 'DataTransfer', 'NOT_READY']
];

const updateTable = "UPDATE db_archiver.tblTasks SET ";
const selectFromTasksTable = "SELECT name, status from db_archiver.tblTasks ";

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
            updateTable + "status = '" + _status + "' WHERE name = '" + _taskName + "'"
        );
        await Tasks.queryAll();
    },
    updateElapsedTime: async (_taskName, _seconds) => {
        await db.queryRecords(
            updateTable + "elapsed_seconds = " + _seconds + " WHERE name = '" + _taskName + "'"
        );
    },
    updateStartTime: async (_taskName, _startedAt) => {
        await db.queryRecords(
            updateTable + "start_time = '" + _startedAt + "' WHERE name = '" + _taskName + "'"
        );
    },
    getTime: () => {
        return moment().format('YYYY-MM-DD HH:mm:ss');
    },
    DataIdentifier: async (_schedule, _callback) => {
        const task_name = 'DataIdentifier';
        // Tasks.printSchedule(_schedule);

        if ( await Tasks.isTaskReady(task_name) ){
            await Tasks.updateStatus(task_name, 'RUNNING');

            // do some work
            await Tasks.updateStartTime(task_name, Tasks.getTime());
            let timeElapsed = await dataIdentifier.execute();

            await Tasks.updateStatus(task_name, 'COMPLETED');
            await Tasks.updateElapsedTime(task_name, timeElapsed[0]);
            await Tasks.updateStatus("DataTransfer", 'READY');
        }
        _callback(task_name);
    },
    DataTransfer: async (_schedule, _callback) => {
        const task_name = 'DataTransfer';

        if ( await Tasks.isTaskReady(task_name) ){
            await Tasks.updateStatus(task_name, 'RUNNING');

            // do some work
            await Tasks.updateStartTime(task_name, Tasks.getTime());
            let timeElapsed = await dataTransfer.execute();

            await Tasks.updateStatus(task_name, 'COMPLETED');
            await Tasks.updateElapsedTime(task_name, timeElapsed[0]);
        }
        _callback(task_name);
    },
    isTaskReady: async (_taskName) => {
        let results = await Tasks.queryStatus(_taskName);
        if ( results[0].status === "READY" ){
            return true;
        }
        return false;
    }
}

module.exports = Tasks;
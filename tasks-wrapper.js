let db = require('./dbService.js');
let dataIdentifier = require('./tasks/data-identifier.js');
let dataTransfer = require('./tasks/data-transfer.js');
let moment = require('moment');

const _tasks = [
    [1, 'DataIdentifier', 'NOT_READY'],
    [2, 'DataTransfer', 'NOT_READY']
];

const updateTable = "UPDATE db_archiver.tblTasks SET ";
const selectFromTable = "SELECT name, status from db_archiver.tblTasks ";

let Tasks = {
    printSchedule: (_schedule) => {
        console.log("\n\ntask scheduled at " + _schedule + " and running at " + new Date());
    },
    reset: async () => {
        await db.createSchema();
        await db.insertRecords(_tasks);
        Tasks.queryAll();
    },
    prepare: async (_taskName) => {
        await Tasks.reset();
        await Tasks.updateStatus(_taskName, 'READY');
    },
    queryAll: async () => {
        let results = await db.queryRecords(
            selectFromTable
        );
        console.table([results[0], results[1]]);
    },
    queryStatus: async (_taskName) => {
        return await db.queryRecords(
            selectFromTable + "WHERE name = '" + _taskName + "'"
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
            updateTable + "elapsedSeconds = " + _seconds + " WHERE name = '" + _taskName + "'"
        );
    },
    updateStartTime: async (_taskName, _startedAt) => {
        await db.queryRecords(
            updateTable + "starttime = '" + _startedAt + "' WHERE name = '" + _taskName + "'"
        );
    },
    DataIdentifier: async (_schedule, _callback) => {
        const task_name = 'DataIdentifier';
        // Tasks.printSchedule(_schedule);

        if ( await Tasks.isTaskReady(task_name) ){
            await Tasks.updateStatus(task_name, 'RUNNING');

            // do some work
            await Tasks.updateStartTime(task_name, moment().format('YYYY-MM-DD HH:mm:ss'));
            let timeElapsed = await dataIdentifier.execute();

            await Tasks.updateStatus(task_name, 'COMPLETED');
            await Tasks.updateElapsedTime(task_name, timeElapsed[0]);
            await Tasks.updateStatus("DataTransfer", 'READY');
        }
        _callback(task_name);
    },
    DataTransfer: async (_schedule, _callback) => {
        const task_name = 'DataTransfer';
        // Tasks.printSchedule(_schedule);

        if ( await Tasks.isTaskReady(task_name) ){
            await Tasks.updateStatus(task_name, 'RUNNING');

            // do some work
            await Tasks.updateStartTime(task_name, moment().format('YYYY-MM-DD HH:mm:ss'));
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
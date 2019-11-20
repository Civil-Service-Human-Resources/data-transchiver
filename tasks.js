let db = require('./dbService.js');

const _tasks = [
    [1, 'DataIdentifier', 'NOT_READY'],
    [2, 'DataTransfer', 'NOT_READY']
];

const sleep =  (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

let Tasks = {
    printSchedule: (_schedule) => {
        console.log("\n\ntask scheduled at " + _schedule + " and running at " + new Date());
    },
    reset: async () => {
        await db.createSchema();
        await db.insertRecords(_tasks);
        Tasks.queryAll();
    },
    prepare: async (taskName) => {
        await Tasks.reset();
        await Tasks.updateStatus(taskName, 'READY');
    },
    queryAll: async () => {
        let results = await db.queryRecords(
            "SELECT name, status from db_archiver.tblTasks"
        );
        console.table([results[0], results[1]]);
    },
    queryStatus: async (taskName) => {
        return await db.queryRecords(
            "SELECT name, status from db_archiver.tblTasks WHERE name = '" + taskName + "'"
        );
    },
    updateStatus: async (taskName, status) => {
        await db.queryRecords(
            "UPDATE db_archiver.tblTasks SET status = '" + status + "' WHERE name = '" + taskName + "'"
        );
        await Tasks.queryAll();
    },
    DataIdentifier: async (_schedule, _callback) => {
        Tasks.printSchedule(_schedule);
        const task_name = 'DataIdentifier';

        if ( await Tasks.isTaskReady(task_name) ){
            await Tasks.updateStatus(task_name, 'RUNNING');

            // do some work
            console.log("Task DataIdentifier is running.....");
            await sleep(10000);
            await Tasks.updateStatus(task_name, 'DONE');
            await Tasks.updateStatus("DataTransfer", 'READY');
        }
        _callback(task_name);
    },
    DataTransfer: async (_schedule, _callback) => {
        Tasks.printSchedule(_schedule);
        const task_name = 'DataTransfer';

        if ( await Tasks.isTaskReady(task_name) ){
            await Tasks.updateStatus(task_name, 'RUNNING');

            // do some work
            console.log("Task DataTransfer is running....");
            await sleep(15000);
            await Tasks.updateStatus(task_name, 'DONE');
        }
        _callback(task_name);
    },
    isTaskReady: async (taskName) => {
        let results = await Tasks.queryStatus(taskName);
        if ( results[0].status === "READY" ){
            return true;
        }
        return false;
    }
}

module.exports = Tasks;
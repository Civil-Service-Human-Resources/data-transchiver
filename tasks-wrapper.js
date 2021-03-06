const db = require('./db/dbService.js');
const dataIdentifier = require('./tasks/data-identifier.js');
const dataTransfer = require('./tasks/data-transfer.js');
const moment = require('moment');

const _tasks = [
    [1, 'DataIdentifier', null, '', 'NOT_READY'],
    [2, 'DataTransfer',   null, '', 'NOT_READY']
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
        try {
            var client = await db.getConnection();
            await client.connect();
            await Tasks.queryAll(client);
            await client.commit();
        }catch(err){
            throw err;
        }finally{
            await db.disconnect(client);
        }
    },
    prepare: async (_taskName) => {
        // db.printConfigs();
        await Tasks.reset();
        await Tasks.updateStatus(_taskName, 'READY');
    },
    queryAll: async (client) => {
        let results = await db.queryRecords(
            selectFromTasksTable,
            client
        );
        console.table([results[0], results[1]]);
    },
    queryStatus: async (_taskName) => {
        try {
            var client = await db.getConnection();
            await client.connect();
            return await db.queryRecords(
                selectFromTasksTable + "WHERE name = '" + _taskName + "'",
                client
            );
        }catch(err){
            throw err;
        }finally{
            await db.disconnect(client);
        }
    },
    updateStatus: async (_taskName, _status) => {
        try {
            var client = await db.getConnection();
            await client.connect();
            await db.queryRecords(
                updateTasksTable + "status = '" + _status + "' WHERE name = '" + _taskName + "'",
                client
            );
            await client.commit();
        }catch(err){
            throw err;
        }finally{
            await db.disconnect(client);
        }
    },
    updateElapsedTime: async (_taskName, _seconds) => {
        try {
            var client = await db.getConnection();
            await client.connect();
            await db.queryRecords(
                updateTasksTable + "elapsed_seconds = " + _seconds + " WHERE name = '" + _taskName + "'",
                client
            );
            await client.commit();
        }catch(err){
            throw err;
        }finally{
            await db.disconnect(client);
        }
    },
    updateStartTime: async (_taskName, _startedAt) => {
        try {
            var client = await db.getConnection();
            await client.connect();
            await db.queryRecords(
                updateTasksTable + "start_time = '" + _startedAt + "' WHERE name = '" + _taskName + "'",
                client
            );
            await client.commit();
        }catch(err){
            throw err;
        }finally{
            await db.disconnect(client);
        }
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
            console.log("Data identifier task finished in " + timeElapsed + " seconds.");

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

            console.log("Data transfer task finished in " + timeElapsed + " seconds.");

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
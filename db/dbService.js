const dbUtil = require('./dbutil.js');
const mongodb = require('mongodb');
const MongoClient = mongodb.MongoClient;

const config_mysql_lr = {
    host     : process.env.TDS_MYSQL_LEARNER_RECORD_DB_HOST,
    user     : process.env.TDS_MYSQL_LEARNER_RECORD_DB_USER,
    password : process.env.TDS_MYSQL_LEARNER_RECORD_DB_PWD,
    multipleStatements: true
};
const config_mysql_target = {
    host     : process.env.TDS_MYSQL_HISTORY_DB_HOST,
    user     : process.env.TDS_MYSQL_HISTORY_DB_USER,
    password : process.env.TDS_MYSQL_HISTORY_DB_PWD,
    multipleStatements: true
};
const config_mysql_registry = {
    host     : process.env.TDS_MYSQL_PROC_REGISTRY_HOST,
    user     : process.env.TDS_MYSQL_PROC_REGISTRY_DB_USER,
    password : process.env.TDS_MYSQL_PROC_REGISTRY_DB_PWD,
    multipleStatements: true
};

const MONGODB_CONNECTION_OPTIONS = "/&retrywrites=false&keepAlive=true&poolSize=10&autoReconnect=true&socketTimeoutMS=60000&connectTimeoutMS=5000";
const cosmos_src_connection_string  = process.env.COSMOS_SRC_CONNECTION_STRING + MONGODB_CONNECTION_OPTIONS;

const updateCandidateTable = "UPDATE db_archiver.candidate_record SET ";

var dbHandler = {
    getConnection: async () => {
        try{
            let con = dbUtil.getMysql(config_mysql_registry);
            return con;
        }catch(err){
            throw err;
        }
    },
    getConnectionToTarget: async () => {
        try{
            let con = dbUtil.getMysql(config_mysql_target);
            return con;
        }catch(err){
            throw err;
        }
    },
    getConnectionToLR: async () => {
        try{
            let con = dbUtil.getMysql(config_mysql_lr);
            return con;
        }catch(err){
            throw err;
        }
    },
    getConnectionToHistoryDB: async () => {
        try{
            let con = dbUtil.getMysql(config_mysql_history);
            return con;
        }catch(err){
            throw err;
        }
    },
    disconnect: async (con) => {
        try{
            await con.close();
        }catch(err){
            throw err;
        }
    },
    createSchema: async () => {
        try{
            var con = await dbHandler.getConnection();
            await con.connect();
            var createTable = `CREATE SCHEMA IF NOT EXISTS db_archiver;
                USE db_archiver;
                CREATE TABLE IF NOT EXISTS tasks_registry(
                id int primary key,
                name varchar(50) not null,
                start_time DATETIME,
                elapsed_seconds varchar(50),
                status varchar(50) not null
            ) ENGINE=InnoDB`;
            await con.query(createTable);
            createTable = `USE db_archiver;
                CREATE TABLE IF NOT EXISTS candidate_record(
                user_id varchar(50) primary key,
                updated_at DATETIME,
                copy_status varchar(50),
                delete_status varchar(50),
                records_transferred BIGINT,
                time_completed DATETIME
            ) ENGINE=InnoDB`;
            await con.query(createTable);
            await con.commit();
        }catch(err){
            throw err;
        }finally{
            await dbHandler.disconnect(con);
        }
    },
    insertRecords: async (_records) => {
        try{
            var con = await dbHandler.getConnection();
            await con.connect();
            await con.query('REPLACE INTO db_archiver.tasks_registry(id, name, status) VALUES ?', [_records]);
            await con.commit();
        }catch(err){
            throw err;
        }finally{
            await dbHandler.disconnect(con);
        }
    },
    insertIntoCandidateRecords: async (_records) => {
        try{
            var con = await dbHandler.getConnection();
            await con.connect();
            await con.query('REPLACE INTO db_archiver.candidate_record(user_id, updated_at) VALUES ?', [_records]);
            await con.commit();
        }catch(err){
            throw err;
        }finally{
            await dbHandler.disconnect(con);
        }
    },
    queryRecords: async (query) => {
        try{
            var con = await dbHandler.getConnection();
            await con.connect();
            var records = await con.query(query);
            return records;
        }catch(err){
            throw err;
        }finally{
            await dbHandler.disconnect(con);
        }
    },
    updateCopyStatus: async (_userId, _status) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "copy_status = '" + _status + "' WHERE user_id = '" + _userId + "'"
        );
    },
    updateDeleteStatus: async (_userId, _status) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "delete_status = '" + _status + "' WHERE user_id = '" + _userId + "'"
        );
    },
    updateNumOfRecords: async (_userId, numRecords) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "records_transferred = " + numRecords + " WHERE user_id = '" + _userId + "'"
        );
    },
    updateTranferTime: async (_userId, completedAt) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "time_completed = '" + completedAt + "' WHERE user_id = '" + _userId + "'"
        );
    },
    queryFromLearnerRecords: async (query) => {
        try{
            var con = await dbHandler.getConnectionToLR();
            await con.connect();
            var records = await con.query(query);
            return records;
        }catch(err){
            throw err;
        }finally{
            await dbHandler.disconnect(con);
        }
    },
    queryFromCosmos: async (user_id) => {
        let db, client;
        try {
            client = await MongoClient.connect(cosmos_src_connection_string,
                { useUnifiedTopology: true, useNewUrlParser: true });
            db = client.db("admin");
            return await db.collection("statements")
                .find({"statement.actor.account.name": user_id})
                .toArray();
        }catch(err){
            throw err
        } finally {
            client.close();
        }
    },
    deleteFromCosmos: async (docsArr) => {
        let db, client;
        try {
            client = await MongoClient.connect(cosmos_src_connection_string,
                { useUnifiedTopology: true, useNewUrlParser: true });
            db = client.db("admin");
            try{
                return await db.collection("statements")
                    .deleteMany({'_id':{'$in':docsArr}});
            }catch(err){
                // fail silently
            }finally {
                client.close();
            }
        }catch(err){
            throw err; 
        }finally {
            client.close();
        }
    },
    copyToTargetMysql: async (_records) => {
        try{
            var con = await dbHandler.getConnectionToTarget();
            await con.connect();
            var result = await con.query(
                'INSERT IGNORE INTO db_archiver.statements_history(statement_hash, user_id, inserted_date, statement_date, statement) VALUES ?', 
                [_records]
            );
            await con.commit();
            return result;
        }catch(err){
            throw err;
        }finally{
            await dbHandler.disconnect(con);
        }
    }
}

module.exports = dbHandler;
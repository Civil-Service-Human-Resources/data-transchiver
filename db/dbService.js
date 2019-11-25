const dbUtil = require('./dbutil.js');
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var Collection = mongodb.Collection;

const config_mysql_lr = {
    host     : process.env.MYSQL_SERVER_HOST,
    user     : process.env.MYSQL_DB_USER,
    password : process.env.MYSQL_DB_PWD,
    multipleStatements: true
};

const cosmos_src_connection_string  = "mongodb://localhost:27017/&retrywrites=false&keepAlive=true&poolSize=10&autoReconnect=true&socketTimeoutMS=600000&connectTimeoutMS=5000";
const cosmos_des_connection_string  = "mongodb://localhost:27017/&retrywrites=false&keepAlive=true&poolSize=10&autoReconnect=true&socketTimeoutMS=600000&connectTimeoutMS=5000";

const config_mysql_registry = {
    host     : process.env.MYSQL_SERVER_HOST,
    user     : process.env.MYSQL_DB_USER,
    password : process.env.MYSQL_DB_PWD,
    multipleStatements: true
};

var dbHandler = {
    getConnection: async () => {
        try{
            let con = dbUtil.getMysql(config_mysql_registry);
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
                CREATE TABLE IF NOT EXISTS tblTasks(
                id int primary key,
                name varchar(50) not null,
                start_time DATETIME,
                elapsed_seconds BIGINT,
                status varchar(50) not null
            ) ENGINE=InnoDB`;
            await con.query(createTable);
            createTable = `USE db_archiver;
                CREATE TABLE IF NOT EXISTS candidate_record(
                user_id varchar(50) primary key,
                last_updated DATETIME,
                copy_status varchar(50),
                delete_status varchar(50),
                num_records_transferred BIGINT,
                time_of_transfer DATETIME
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
            await con.query('REPLACE INTO db_archiver.tblTasks(id, name, status) VALUES ?', [_records]);
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
            await con.query('REPLACE INTO db_archiver.candidate_record(user_id, last_updated) VALUES ?', [_records]);
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
    writeToCosmos: async (docs) => {
        let db, client;
        try {
            client = await MongoClient.connect(cosmos_des_connection_string,
                { useUnifiedTopology: true, useNewUrlParser: true });
            db = client.db("archive");
            try{
                return await db.collection("statements")
                    .insertMany(docs, {ordered: false});
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
    }
}

module.exports = dbHandler;
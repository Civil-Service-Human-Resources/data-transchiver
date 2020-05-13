const dbUtil = require('./dbutil.js');
const dbCredentials = require('../constants/dbCredentials.js');

const updateCandidateTable = "UPDATE db_archiver.candidate_record SET ";

var dbHandler = {
    printConfigs: () => {
        console.info("config_mysql_lr            => " + JSON.stringify(dbCredentials.config_mysql_lr, null, 2));
        console.info("config_mysql_target        => " + JSON.stringify(dbCredentials.config_mysql_target, null, 2));
        console.info("config_mysql_registry      => " + JSON.stringify(dbCredentials.config_mysql_registry, null, 2));
        console.info("MONGODB_CONNECTION_OPTIONS => " + JSON.stringify(dbCredentials.cosmos_src_connection_string, null, 2));
    },
    getConnection: async () => {
        try{
            let con = dbUtil.getMysql(dbCredentials.config_mysql_registry);
            return con;
        }catch(err){
            throw err;
        }
    },
    getConnectionToTarget: async () => {
        try{
            let con = dbUtil.getMysql(dbCredentials.config_mysql_target);
            return con;
        }catch(err){
            throw err;
        }
    },
    getConnectionToLR: async () => {
        try{
            let con = dbUtil.getMysql(dbCredentials.config_mysql_lr);
            return con;
        }catch(err){
            throw err;
        }
    },
    getConnectionToHistoryDB: async () => {
        try{
            let con = dbUtil.getMysql(dbCredentials.config_mysql_history);
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
            ) ENGINE=InnoDB;
            DROP TABLE IF EXISTS tasks_registry_prev;
            CREATE TABLE tasks_registry_prev AS SELECT * FROM tasks_registry;
            TRUNCATE TABLE tasks_registry;`;
            await con.query(createTable);
            var createIndexAddingProcedure = `USE db_archiver;
                DROP PROCEDURE IF EXISTS csi_add_index;
                CREATE PROCEDURE csi_add_index(in theTable varchar(128), in theIndexName varchar(128), in theIndexColumns varchar(128)  )
                BEGIN
                    IF((SELECT COUNT(*) AS index_exists FROM information_schema.statistics WHERE TABLE_SCHEMA = DATABASE() and table_name = theTable AND index_name = theIndexName)  = 0) THEN
                        SET @s = CONCAT('CREATE INDEX ' , theIndexName , ' ON ' , theTable, '(', theIndexColumns, ')');
                        PREPARE stmt FROM @s;
                        EXECUTE stmt;
                    END IF;
                END;`;
            await con.query(createIndexAddingProcedure);
            await con.commit();
            createTable = `USE db_archiver;
                CREATE TABLE IF NOT EXISTS candidate_record(
                user_id varchar(50) primary key,
                updated_at DATETIME,
                copy_status varchar(50),
                delete_status varchar(50),
                copied_count BIGINT,
                deleted_count BIGINT,
                time_completed DATETIME
            ) ENGINE=InnoDB;
                CALL csi_add_index('candidate_record', 'candidate_record_updated_at_idx', 'updated_at DESC');
                DROP TABLE IF EXISTS candidate_record_prev;
                CREATE TABLE candidate_record_prev AS SELECT * FROM candidate_record;`;
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
            await con.query('REPLACE INTO db_archiver.tasks_registry(id, name, start_time, elapsed_seconds, status) VALUES ?', [_records]);
            await con.commit();
        }catch(err){
            throw err;
        }finally{
            await dbHandler.disconnect(con);
        }
    },
    insertIntoCandidateRecords: async (_records, client) => {
        await client.query('REPLACE INTO db_archiver.candidate_record(user_id, updated_at) VALUES ?', [_records]);
    },
    queryRecords: async (query, client) => {
            var records = await client.query(query);
            return records;
    },
    updateCopyStatus: async (_userId, _status, client) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "copy_status = '" + _status + "' WHERE user_id = '" + _userId + "'",
            client
        );
    },
    updateDeleteStatus: async (_userId, _status, client) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "delete_status = '" + _status + "' WHERE user_id = '" + _userId + "'",
            client
        );
    },
    updateNumOfRecordsCopied: async (_userId, numRecords, client) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "copied_count = " + numRecords + " WHERE user_id = '" + _userId + "'",
            client
        );
    },
    updateNumOfRecordsDeleted: async (_userId, numRecords, client) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "deleted_count = " + numRecords + " WHERE user_id = '" + _userId + "'",
            client
        );
    },
    updateTranferTime: async (_userId, completedAt, client) => {
        await dbHandler.queryRecords(
            updateCandidateTable + "time_completed = '" + completedAt + "' WHERE user_id = '" + _userId + "'",
            client
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
    queryFromCosmos: async (user, client) => {
        let db;
        db = client.db("admin");
        return await db.collection("statements")
            .find({
                "statement.actor.account.name": user.user_id, 
                "statement.timestamp": { $lt: user.updated_at }
            })
            .toArray();
    },
    deleteFromCosmos: async (docsArr, client) => {
        let db;
        db = client.db("admin");
        return await db.collection("statements")
                .deleteMany({'_id':{'$in':docsArr}});
    },
    copyToTargetMysql: async (_records, client) => {
        var result = await client.query(
            'REPLACE INTO db_archiver.statements_history(statement_hash, user_id, inserted_date, statement_date, statement) VALUES ?', 
            [_records]
        );
        await client.commit();
        return result;
    }
}

module.exports = dbHandler;
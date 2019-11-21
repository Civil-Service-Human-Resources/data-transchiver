const dbUtil = require('./dbutil.js');

var dbHandler = {
    getConnection: async () => {
        try{
            var config = {
                host     : process.env.MYSQL_SERVER_HOST,
                user     : process.env.MYSQL_DB_USER,
                password : process.env.MYSQL_DB_PWD,
                multipleStatements: true
            }            
            con = dbUtil.getMysql(config);
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
                starttime DATETIME,
                elapsedSeconds BIGINT,
                status varchar(50) not null
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
    }
}

module.exports = dbHandler;
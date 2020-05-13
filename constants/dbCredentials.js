let dbCredentials = {};

dbCredentials.config_mysql_lr = {
    connectionLimit : 100,
    host     : process.env.TDS_MYSQL_LEARNER_RECORD_DB_HOST,
    user     : process.env.TDS_MYSQL_LEARNER_RECORD_DB_USER,
    password : process.env.TDS_MYSQL_LEARNER_RECORD_DB_PWD,
    multipleStatements: true,
    ssl: {
        rejectUnauthorized: false
    }
};

dbCredentials.config_mysql_target = {
    connectionLimit : 100,
    host     : process.env.TDS_MYSQL_HISTORY_DB_HOST,
    user     : process.env.TDS_MYSQL_HISTORY_DB_USER,
    password : process.env.TDS_MYSQL_HISTORY_DB_PWD,
    multipleStatements: true,
    ssl: {
        rejectUnauthorized: false
    }
};

dbCredentials.config_mysql_registry = {
    connectionLimit : 100,
    host     : process.env.TDS_MYSQL_PROC_REGISTRY_HOST,
    user     : process.env.TDS_MYSQL_PROC_REGISTRY_DB_USER,
    password : process.env.TDS_MYSQL_PROC_REGISTRY_DB_PWD,
    multipleStatements: true,
    ssl: {
        rejectUnauthorized: false
    }
};

dbCredentials.MONGODB_CONNECTION_OPTIONS = "/&retrywrites=false&keepAlive=true&poolSize=10&autoReconnect=true&socketTimeoutMS=60000&connectTimeoutMS=5000";
dbCredentials.cosmos_src_connection_string = process.env.COSMOS_SRC_CONNECTION_STRING + dbCredentials.MONGODB_CONNECTION_OPTIONS;

module.exports = dbCredentials;
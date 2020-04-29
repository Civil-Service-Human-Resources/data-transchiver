module.exports = Object.freeze({
    config_mysql_lr : {
        host     : process.env.TDS_MYSQL_LEARNER_RECORD_DB_HOST,
        user     : process.env.TDS_MYSQL_LEARNER_RECORD_DB_USER,
        password : process.env.TDS_MYSQL_LEARNER_RECORD_DB_PWD,
        multipleStatements: true,
        ssl: {
            rejectUnauthorized: false
        }
    },
    config_mysql_target : {
        host     : process.env.TDS_MYSQL_HISTORY_DB_HOST,
        user     : process.env.TDS_MYSQL_HISTORY_DB_USER,
        password : process.env.TDS_MYSQL_HISTORY_DB_PWD,
        multipleStatements: true,
        ssl: {
            rejectUnauthorized: false
        }
    },
    config_mysql_registry : {
        host     : process.env.TDS_MYSQL_PROC_REGISTRY_HOST,
        user     : process.env.TDS_MYSQL_PROC_REGISTRY_DB_USER,
        password : process.env.TDS_MYSQL_PROC_REGISTRY_DB_PWD,
        multipleStatements: true,
        ssl: {
            rejectUnauthorized: false
        }
    },
    MONGODB_CONNECTION_OPTIONS: "/&retrywrites=false&keepAlive=true&poolSize=10&autoReconnect=true&socketTimeoutMS=60000&connectTimeoutMS=5000",
    cosmos_src_connection_string: process.env.COSMOS_SRC_CONNECTION_STRING + dbCredentials.MONGODB_CONNECTION_OPTIONS
});
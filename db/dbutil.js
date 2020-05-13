const mysql = require('mysql');
const util = require('util');

let getMysql = ( config ) => {
    const pool = mysql.createPool( config );

    console.log("Pool: ")
    console.log(pool);

    return {
      query: util.promisify( pool.query ).bind( pool ),
      close: util.promisify( pool.end ).bind( pool )
    };
}

module.exports.getMysql = getMysql;
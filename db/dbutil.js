const mysql = require('mysql');
const util = require('util');

let getMysql = ( config ) => {
    const connection = mysql.createPool( config );

    return {
      query: util.promisify( connection.query ).bind( connection ),
      close: util.promisify( connection.end ).bind( connection ),
      connect: util.promisify( connection.connect ).bind( connection ),
      commit: util.promisify( connection.commit ).bind( connection )
    };
}

module.exports.getMysql = getMysql;
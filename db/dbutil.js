const mysql = require('mysql');
const util = require('util');

function reconnect(connection, config){
  console.log("New connection tentative...");

  if (connection) {
    connection.destroy();
  }

  var connection = mysql.createConnection(config);

  connection.connect(function(error){
      if (error) {
          setTimeout(reconnect, 2000);
      } else {
          console.log("New connection established with the database.")
          return connection;
      }
  });
}

let getMysql = ( config ) => {
    const connection = mysql.createConnection( config );

    connection.on('error', function(error) {
      console.log("Cannot establish a connection with the database: " + error.code);

      if (err.code === "PROTOCOL_ENQUEUE_AFTER_QUIT" || err.code === "ECONNRESET") {
        connection = reconnect(connection, config);
      }
    });

    return {
      query: util.promisify( connection.query ).bind( connection ),
      close: util.promisify( connection.end ).bind( connection ),
      connect: util.promisify( connection.connect ).bind( connection ),
      commit: util.promisify( connection.commit ).bind( connection )
    };
}

module.exports.getMysql = getMysql;
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
    var connection = mysql.createConnection( config );

    console.log("Setting connection listener: ");

    connection.on('error', function(error) {
      console.log("Inside error connection listener.");
      if (error.code === "PROTOCOL_ENQUEUE_AFTER_QUIT" || 
          error.code === "PROTOCOL_CONNECTION_LOST" ||
          error.code === "PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR" ||
          error.code === "ECONNRESET") {
        console.log("Cannot establish a connection with the database: " + error.code);
        connection = reconnect(connection, config);
      } else if (error.code === "PROTOCOL_ENQUEUE_HANDSHAKE_TWICE") {
        console.log("Connection already established");
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
// package.json에 추가한 dotenv모듈 import
require("dotenv").config();
const express = require("express"); // for building rest apis 
const cors = require("cors"); // provides Express middleware to enable CORS with various options
var WebSocketServer = require('websocket').server;
var http = require('http');
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka:9092', 'kafka2:9092'],
})
const consumer = kafka.consumer({ groupId: 'test-group' })


const app = express();

var corsOptions = {
  origin: "http://localhost:8081"
};

app.use(cors(corsOptions));

// parse requests of content-type - application/json
app.use(express.json());

// parse requests of content-type - application/x-www-form-urlencoded
app.use(express.urlencoded({ extended: true }));

// configure mongodb DB & mongoose
    // 2. connect() after index.js
// const db = require("./app/models");

// console.log(db.url);

// db.mongoose
//   .connect(db.url, {
//     useNewUrlParser: true,
//     useUnifiedTopology: true
//   })
//   .then(() => {
//     console.log("Connected to the database!");
//   })
//   .catch(err => {
//     console.log("Cannot connect to the database!", err);
//     process.exit();
//   });



// require("./app/routes/turorial.routes")(app);


var server = http.createServer(function(request, response) {
  console.log(' Request recieved : ' + request.url);
  response.writeHead(404);
  response.end();
 });
 server.listen(8081, function() {
  console.log('Listening on port : 8081');
 });
  
 webSocketServer = new WebSocketServer({
  httpServer: server,
  autoAcceptConnections: false
 });
  
 function iSOriginAllowed(origin) {
  return true;
 }
  
 webSocketServer.on('request', function(request) {
  if (!iSOriginAllowed(request.origin)) {
  request.reject();
  console.log(' Connection from : ' + request.origin + ' rejected.');
  return;
  }
  
  var connection = request.accept('echo-protocol', request.origin);
  console.log(' Connection accepted : ' + request.origin);
  connection.on('message', function(message) {
  if (message.type === 'utf8') {
  console.log('Received Message: ' + message.utf8Data);
  }
  });
  consumer.connect()
  consumer.subscribe({ topic: 'movie', fromBeginning: true })
  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
    },
  })
  connection.on('close', function(reasonCode, description) {
  console.log('Connection ' + connection.remoteAddress + ' disconnected.');
  });
 });

// set port, listen for requests
// 위에 dotenv 임포트한거 사용해서 port setting에 process.env 사용
const PORT = process.env.NODE_DOCKER_PORT || 8081;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}.`);
});

'use strict'
// 카프카
const kafka = require('kafka-node');

const Consumer = kafka.Consumer,
  client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'}),
  consumer = new Consumer(
  client, [ { topic: 'movies'} ], { autoCommit: false });

const admin = new kafka.Admin(client);
admin.listTopics((err, res) => {
    console.log('topics', res);
});
// consumer.on('message', (message) => {
//     console.log(message.value);
// });

// 소켓
const http = require('http')
const path = require('path')
const EventEmitter = require('events')
const express = require('express')
const socketio = require('socket.io')
const port = process.env.PORT || 8888

const app = express()
const server = http.createServer(app)
const io = socketio(server)
const events = new EventEmitter()

app.use(express.static(path.join(__dirname)))
  

io.on('connection', socket => {
    console.log("socket connected");
  consumer.on('message', function (message) {
      socket.emit('event', message.value);
      console.log(message.value);
    });
  
});

server.listen(port, () => console.log(`Listening on port ${port}`))
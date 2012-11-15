var EventEmitter   = require("events").EventEmitter
  , util           = require('util')
  , amqp           = require('./amqp-plus')
  , common         = require('./common')
  , AgentClient    = require('./client')

var Agent = module.exports = function (config) {
  this.config = config || { amqp: {} };
  this.uuid = config.uuid;
}

util.inherits(Agent, EventEmitter);

Agent.prototype.configureAMQP = common.configureAMQP;

Agent.prototype.connect = function (queues, callback) {
  var self = this;

  self.connection = amqp.createConnection(
      self.config.amqp, { log: this.config.log });
  self.connection.on('ready', self.onReady.bind(self));
  self.connection.reconnect();
}

Agent.prototype.onReady = function () {
  var self = this;
  console.info("Ready to receive commands");
  self.connected = true;
  self.exchange = self.connection.exchange('amq.topic', { type: 'topic' });

  var nopMsgInterval = setInterval(publishNOP, 30000);

  self.emit('ready');

  function publishNOP() {
    if (!self.connected) {
      clearInterval(nopMsgInterval);
      return;
    }
    self.exchange.publish(self.resource + '._nop.' + self.uuid, {});
  }
};


/**
 * Callsback with an agent handle we can use to send commands to other agents.
 */
Agent.prototype.getLocalAgentHandle = function (type, callback) {
  var self = this;

  // Return an existing handle if available.
  if (self.agentHandles && self.agentHandles[type]) {
    return callback(null, self.agentHandles[type]);
  }

  if (!self.agentClient) {
    var config = { timeout: 600000 };
    self.agentClient = new AgentClient(config);
    self.agentClient.useConnection(self.connection, function () {
      setupHandles();
    });
  }
  else {
    setupHandles();
  }

  function setupHandles () {
    self.agentClient.getAgentHandle(type, self.uuid, function (handle) {
      if (!self.agentHandles) self.agentHandles = {};
      self.agentHandles[type] = handle;
      callback(null, handle);
    });
  }
}

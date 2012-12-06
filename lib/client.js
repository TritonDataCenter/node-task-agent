// send a command to:
//   $resource.request.$command.$uuid

var amqp = require('amqp-plus');
var common = require('./common');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

function Client(config) {
    this.config = config = config || { amqp: {} };
    this.commandTimeout = config.timeout || 5000;
    this.maxTasks = config.maxTasks || 32;
    this.config.reconnect = config.reconnect || false;
    this.verbose = config.verbose;
    this.log = config.log;
}

Client.prototype.configureAMQP = common.configureAMQP;

Client.prototype.connect = function (callback) {
    var self = this;

    this.connection = amqp.createConnection(
        this.config.amqp,
        {
            attemptToReconnect: self.config.attemptToReconnect,
            log: self.config.log
        });
    this.connection.reconnect();

    // Set up the exchange we'll be using to publish our commands. We wait for
    // the exchange to open and then run the callback.
    this.connection.addListener('ready', function () {
        self.onConnect();
        callback();
    });
};

Client.prototype.useConnection = function (connection) {
    var self = this;
    self.connection = connection;
    self.connection.on('ready', self.onConnect.bind(self));
};

Client.prototype.onConnect = function () {
    var self = this;
    self.agentHandles = {};
    self.exchange = self.connection.exchange('amq.topic', { type: 'topic' });
};

Client.prototype.end = function () {
    this.config.reconnect = false;
    this.connection.end();
};

/**
 * The 'Client' object is decoupled from the creation and management of queues
 * used to communicate with the agents. We will have the Client object hand us
 * handles/closures/whatever that will deal with their own objects.
 */

Client.prototype.getAgentHandle = function (resource, uuid, callback) {
    var self = this;
    var handle;

    if (!this.agentHandles) {
        this.agentHandles = {};
    }

    if (this.agentHandles[uuid]) {
        self.log.debug('Reuising agent handle for %s', uuid);
        handle = this.agentHandles[uuid];
        callback(handle);
        return;
    } else {
        self.log.debug('Creating agent handle for %s', uuid);
        handle =
            this.agentHandles[uuid] = new AgentHandle({
                log:        this.config.log,
                connection: this.connection,
                exchange:   this.exchange,
                uuid:       uuid,
                timeout:    this.commandTimeout,
                maxTasks:   this.maxTasks,
                resource:   resource
        });

        handle.prepareAgentEventQueue(function () {
            callback(handle);
        });
    }
};

function AgentHandle(args) {
    this.uuid = args.uuid;
    this.connection = args.connection;
    this.exchange = args.exchange;
    this.clientId = common.genId();
    this.resource = args.resource;
    this.commandTimeout = args.timeout;
    this.taskHandles = {};
    this.log = args.log;
    this.maxTasks = args.maxTasks;
}

AgentHandle.prototype.prepareAgentEventQueue = function (callback) {
    var self = this;

    var queueName = common.dotjoin(
                        this.resource + '-client', this.uuid, 'events',
                        common.genId());

    this.log.warn('Waiting for task events on queue: ' + queueName);
    var queue = this.connection.queue(
                    queueName, { autoDelete: true }, queueCallback);

    function queueCallback() {
        var rk = common.dotjoin(
                    self.resource, '*', 'event', '*', self.clientId, '*');

        console.warn('Binding to: ' + rk);
        queue.bind('amq.topic', rk);

        queue.subscribe(function (msg, headers, deliveryInfo) {
            var rkParts = deliveryInfo.routingKey.split('.');
            var eventType = rkParts[3];
            var taskId = rkParts[5];
            if (self.taskHandles[taskId]) {
                self.taskHandles[taskId].emit('event', eventType, msg);
                if (eventType === 'finish') {
                    clearTimeout(self.taskHandles[taskId].timeout);
                    delete self.taskHandles[taskId];
                    self.log.debug('Deleted task handle (space for %d now)',
                        self.maxTasks - Object.keys(self.taskHandles).length);
                }
            }
        });
        callback();
    }
};

function TaskHandle(id) {
    EventEmitter.call(this);
    this.id = id;
    this.timeout = null;
}

util.inherits(TaskHandle, EventEmitter);

AgentHandle.prototype.sendTask = function (task, msg, callback) {
    var self = this;

    self.log.info('Sending task to %s', self.uuid);
    msg.task_id = common.genId();
    msg.client_id = self.clientId;

    if (Object.keys(this.taskHandles).length > this.maxTasks) {
        self.log.warn('Hit the limit (%d) of tasks limit for %s',
            this.maxTasks, self.uuid);
        callback();
        return;
    } else {
        self.log.info('%d/%d concurrent tasks',
            Object.keys(this.taskHandles).length, this.maxTasks);
    }

    var taskHandle
        = this.taskHandles[msg.task_id]
        = new TaskHandle(msg.task_id);

    var routingKey = common.dotjoin(this.resource, this.uuid, 'task', task);

    self.log.info(
        { message: msg },
        'Publishing message to routing key: "' + routingKey + '"');
    self.exchange.publish(routingKey, msg);
    taskHandle.timeout.setTimeout(function () {
        self.log.error(
            'Task (%s) to (%s) timed out after %d seconds.',
            task, self.uuid);
        delete self.taskHandles[msg.task_id];
        self.log.debug('Deleted task handle (space for %d now)',
            self.maxTasks - Object.keys(self.taskHandles).length);
    }, self.timeout * 1000);
    callback(taskHandle);
};

module.exports = Client;

var util           = require('util');
var path           = require('path');
var Agent          = require('./agent');
var ThrottledQueue = require('./throttled_queue');
var common         = require('./common');
var TaskRunner     = require('./task_runner');
var bunyan         = require('bunyan');

function TaskAgent(config) {
    if (!config.resource) {
        throw new Error(
            'configuration parameter "resource" must be specified');
    }

    this.tasklogdir = config.tasklogdir;
    this.log = bunyan.createLogger({ name: config.logname });
    config.log = this.log;
    Agent.call(this, config);

    if (config.tasksPath) {
        this.tasksPath = config.tasksPath;
    } else {
        this.log.warn(
            'Warning: no taskPaths specified when instantiating TaskAgent');
        this.tasksPath = path.join(__dirname, '..', 'tasks');
    }
    this.resource = config.resource;
    this.runner = new TaskRunner({
        log: this.log,
        logdir: this.tasklogdir,
        tasksPath: this.tasksPath
    });
}

util.inherits(TaskAgent, Agent);

TaskAgent.prototype.useQueues = function (defns) {
    var self = this;

    defns.forEach(function (queueDefn) {
        var routingKeys
            = queueDefn.tasks.map(function (t) {
                return [
                    self.resource,
                    self.uuid,
                    'task',
                    t
                ].join('.');
            });

        var queueName = [self.resource, self.uuid, queueDefn.name].join('.');
        var queue;

        function callback(msg, headers, deliveryInfo) {
            var rkParts = deliveryInfo.routingKey.split('.');

            self.log.info(
                'Incoming routing key was: %s', deliveryInfo.routingKey);
            self.log.info('Incoming message was: ');
            self.log.info(util.inspect(msg, true, 10));

            var task     = rkParts[3];
            var clientId = msg.client_id;
            var taskId   = msg.task_id;

            var request = {
                finish:    finish,
                task:      task,
                params:    msg,
                event:     event,
                progress:  progress
            };

            queueDefn.onmsg(request);

            function finish() {
                queue.complete();
            }

            function progress(value) {
                event('progress', { value: value });
            }

            function event(name, message) {
                var rk
                    = common.dotjoin(
                          self.resource,
                          self.uuid,
                          'event',
                          name,
                          clientId,
                          taskId);

                self.log.info('Publishing event to routing key (' + rk + '):');
                self.log.debug(util.inspect(message, true, 10));
                self.exchange.publish(rk, message);
            }
        }

        self.log.debug('Binding to queue (' + queueName + ') routing keys: ');
        self.log.debug(util.inspect(routingKeys, true, 10));

        var queueOptions = {
            connection:  self.connection,
            queueName:   queueName,
            routingKeys: routingKeys,
            callback:    callback,
            maximum:     queueDefn.maxConcurrent,
            log:         self.log
        };
        queue = new ThrottledQueue(queueOptions);
        queue.next();
    });
};

TaskAgent.prototype.setupPingQueue = function (taskQueues) {
    var self = this;
    var queueName = this.resource + '.ping.' + this.uuid;
    var queue = this.connection.queue(queueName);

    queue.addListener('open', function (messageCount, consumerCount) {
        queue.bind(
            'amq.topic', self.resource + '.ping.' + self.uuid);
        queue.subscribe({ ack: true }, function (msg, headers, deliveryInfo) {
            self.log.info('Received ping message');
            var client_id = msg.client_id;
            var id = msg.id;

            msg = {
                req_id: id,
                timestamp: new Date().toISOString()
            };
            var routingKey = self.resource + '.ack'
                                + client_id + '.' + self.uuid;

            self.log.info('Publishing ping reply to ' + routingKey);
            self.exchange.publish(routingKey, msg);

            queue.shift();
        });
    });
};

TaskAgent.prototype.setupQueues = function (taskQueues) {
    var self = this;

    self.setupPingQueue();

    var taskManagementQueues = [
        {
            name: 'task_management',
            maxConcurrent: 8,
            tasks: [ 'show_tasks' ],
            onmsg: function (req) {
                var history = self.runner.taskHistory;
                var i;

                for (i = history.length; i--; ) {
                    var entry = history[i];
                    var started_at = new Date(entry.started_at);
                    var finished_at = entry.finished_at
                        ? new Date(entry.finished_at)
                        : new Date();
                    entry.elapsed_seconds = (finished_at - started_at) / 1000;
                }

                req.event('finish', { history: history });
                req.finish();
            }
        }
    ];

    self.useQueues(taskManagementQueues);
    self.useQueues(taskQueues);
};

module.exports = {
    TaskAgent: TaskAgent
};

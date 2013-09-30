var util = require('util');
var path = require('path');
var Agent = require('./agent');
var ThrottledQueue = require('./throttled_queue');
var common = require('./common');
var TaskRunner = require('./task_runner');
var bunyan = require('bunyan');
var restify = require('restify');
var os = require('os');
var async = require('async');

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

TaskAgent.prototype.start = function () {
    this.connect();
    this.startHttpService();
};

TaskAgent.prototype.startHttpService = function (defns) {
    var self = this;

    var server = self.httpserver = restify.createServer({
        log: this.log,
        name: 'Provisioning API'
    });

    server.use(restify.requestLogger());
    server.use(restify.acceptParser(server.acceptable));
    server.use(restify.authorizationParser());
    server.use(restify.dateParser());
    server.use(restify.queryParser());
    server.use(restify.bodyParser());

    server.on('after', function (req, res, route, err) {
        var method = req.method;
        var reqpath = req.path();
        if (method === 'GET' || method === 'HEAD') {
            if (reqpath === '/ping') {
                return;
            }
        }
        // Successful GET res bodies are uninteresting and *big*.
        var body = method !== 'GET' &&
                   res.statusCode !== 404 &&
                   Math.floor(res.statusCode/100) !== 2;

        restify.auditLogger({
            log: req.log.child({ route: route }, true),
            body: body
        })(req, res, route, err);
    });

    server.use(function (req, res, next) {
        res.on('header', function onHeader() {
            var now = Date.now();
            res.header('Date', new Date());
            res.header('Server', server.name);
            res.header('x-request-id', req.getId());
            var t = now - req.time();
            res.header('x-response-time', t);
            res.header('x-server-name', os.hostname());
        });
        next();
    });

    server.on('uncaughtException', function (req, res, route, err) {
        req.log.error(err);
        res.send(err);
    });

    // Need a proper "model" in which to track the administrative details of
    // executing taks (status, progress, history, etc).

    server.post('/tasks', function (req, res, next) {
        if (!req.params.hasOwnProperty('task')) {
            next(new restify.InvalidArgumentError(
                'Missing key \'task\''));
            return;
        }

        var dispatch = {};

        self.queueDefns.forEach(function (i) {
            i.tasks.forEach(function (j) {
                dispatch[j] = i.onhttpmsg;
            });
        });

        var value, error;

        var cbcount = 0;
        function fcb() {
            cbcount++;

            if (cbcount === 2) {
                if (error) {
                    res.send(500, error);
                    next();
                    return;
                }
                res.send(200, value);
                next();
            }
        }

        var params = {
            task: req.params.task,
            params: req.params,

            finish: function () {
                fcb();
            },
            progress: function (v) {
            },
            event: function (name, message) {
                self.log.trace(
                    { name: name, message: message }, 'Received event');
                if (name === 'finish') {
                    value = message;
                    fcb();
                } else if (name === 'error') {
                    error = message;
                }
            }
        };

        // NEED TO CALL DISPATCH FN WITH A "REQ" OBJECT
        dispatch[req.params.task](params);
    });

    server.get('/tasks', function (req, res, next) {
        var opts = {};

        if (req.params.status) {
            opts.status = req.params.status;
        }
        var history = getHistory(opts);
        res.send(200, history);
        next();
    });

    function getHistory(opts) {
        var history = self.runner.taskHistory;
        var i;

        for (i = history.length; i--; ) {
            var entry = history[i];
            if (opts.status && opts.status !== entry.status) {
                continue;
            }
            var started_at = new Date(entry.started_at);
            var finished_at = entry.finished_at
                ? new Date(entry.finished_at)
                : new Date();
            entry.elapsed_seconds = (finished_at - started_at) / 1000;
        }

        return history;
    }

    server.get('/history', function (req, res, next) {
        var history = self.runner.taskHistory;
        var i;

        for (i = history.length; i--; ) {
            var entry = history[i];
            if (entry.status !== 'active') {
                continue;
            }
            var started_at = new Date(entry.started_at);
            var finished_at = entry.finished_at
                ? new Date(entry.finished_at)
                : new Date();
            entry.elapsed_seconds = (finished_at - started_at) / 1000;
        }

        res.send(200, history);
        next();
    });

    self.httpserver.listen(5309, function () {
        self.log.info(
            '%s listening at %s', self.httpserver.name, self.httpserver.url);
    });
};

TaskAgent.prototype.useQueues = function (defns) {
    var self = this;

    self.queueDefns = defns;

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
                var rk = common.dotjoin(
                            self.resource,
                            self.uuid,
                            'event',
                            name,
                            clientId,
                            taskId);

                self.log.info({ req_id: msg.req_id, message: message},
                              'Publishing event to routing key (' + rk + ')');
                self.exchange.publish(rk, message);
            }
        }

        self.log.debug('Binding to queue (' + queueName + ') routing keys: ');
        self.log.debug(util.inspect(routingKeys, true, 10));

        var queueOptions = { 'arguments': {} };
        if (queueDefn.expires) {
            queueOptions.arguments['x-message-ttl'] = queueDefn.expires * 1000;
        }

        var options = {
            connection:   self.connection,
            queueName:    queueName,
            routingKeys:  routingKeys,
            callback:     callback,
            maximum:      queueDefn.maxConcurrent,
            log:          self.log,
            queueOptions: queueOptions
        };
        queue = new ThrottledQueue(options);
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

module.exports = TaskAgent;

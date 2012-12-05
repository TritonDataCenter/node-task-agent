/*
 * This class, TaskRunner, is responsible for starting the child process
 * (found in task_worker). It also propagates events to and from the child
 * process.
 */

var util = require('util');
var path = require('path');
var fs = require('fs');
var fork = require('child_process').fork;
var sprintf = require('sprintf').sprintf;

function isString(obj) {
    return Object.prototype.toString.call(obj) === '[object String]';
}


function TaskRunner(options) {
    this.tasksPath = options.tasksPath;
    this.taskHistory = [];
    this.children = {};
    this.logdir = options.logdir;
    this.log = options.log;

    if (!fs.existsSync(this.logdir)) {
        fs.mkdirSync(this.logdir, parseInt('0755', 8));
    }
}


util.inherits(TaskRunner, process.EventEmitter);

var MAXIMUM_MESSAGE_STRING_LENGTH = 1000;

function cloneTruncated(obj, length) {
    var i, out;
    if (Array.isArray(obj)) {
        out = [];
        var len = obj.length;
        for (i = 0; i < len; i++) {
            out[i] = arguments.callee(obj[i], length);
        }
        return out;
    }
    if (typeof (obj) === 'object') {
        out = {};
        for (i in obj) {
            if (isString(obj) && obj.length > length) {
                out[i] = obj[i].substr(0, length);
            } else {
                out[i] = arguments.callee(obj[i], length);
            }
        }
        return out;
    }
    return obj;
}


TaskRunner.prototype.dispatch = function (req) {
    var self = this;

    this.logname = sprintf(
        '%s-%s.log',
        (new Date()).toISOString()
            .replace(/[-:]/g, '')
            .replace(/\.\d+Z$/g, 'Z'),
        req.task);

    var taskModule = path.join(self.tasksPath, req.task);
    var env = JSON.parse(JSON.stringify(process.env));

    env.logfile = self.logdir + '/' + self.logname;

    self.log.info('Child logging to %s', env.logfile);
    var child = fork(
        __dirname + '/task_worker.js',
        [taskModule],
        { env: env });

    var pid = child.pid;

    function logForChild(level, message) {
        message = cloneTruncated(message, MAXIMUM_MESSAGE_STRING_LENGTH);
        if (isString(message)) {
            self.log[level]('pid:' + pid + ' - ' + message);
        } else {
            self.log[level](
                'pid:' + pid + ' - ' + util.inspect(message, null, Infinity));
        }
    }

    function info(message) {
        logForChild('info', message);
    }

    function debug(message) {
        logForChild('debug', message);
    }

    function error(message) {
        logForChild('error', message);
    }

    function warn(message) {
        logForChild('warn', message);
    }

    info('Executing task module: ' + taskModule);

    var entry = {};
    this.children[pid] = child;
    this.taskHistory.push(entry);

    entry.started_at = (new Date().toISOString());
    entry.task = req.task;
    entry.pid = pid;
    entry.params = req.params;
    entry.status = 'active';
    entry.errorCount = 0;
    entry.messages = [];
    entry.log = [];

    child.on('message', function (msg) {
        debug('Parent received hydracp ' + msg.type
            + ' message from child process.');
        if (msg.type !== 'log') {
            debug(msg);
        }

        msg.timestamp = new Date();

        switch (msg.type) {
            case 'ready':
                info('Received "ready" event.');
                info('Sending "start" event with payload to child.');
                child.send({
                    action: 'start',
                    req: req,
                    tasksPath: self.tasksPath
                });
                break;

            case 'event':
                entry.messages.push(msg);
                info('Received a task event from child task process: '
                    + msg.name);
                debug(msg.event);

                if (msg.name === 'error') {
                    entry.errorCount++;
                }

                switch (msg.name) {
                    case 'progress':
                        child.emit('progress', msg.event.value);
                        break;

                    case 'finish':
                        entry.finished_at = (new Date().toISOString());
                        entry.status = 'finished';
                        child.emit('finish');
                        child.emit('event', msg.name, msg.event);
                        child.kill();
                        break;

                    default:
                        child.emit('event', msg.name, msg.event);
                        break;
                }
                break;

            case 'subtask':
                entry.messages.push(msg);
                child.emit('subtask', msg.id, msg.resource, msg.task, msg.msg);
                break;

            case 'exception':
                entry.messages.push(msg);
                error('Uncaught exception in child: ');
                error(msg.error.stack);
                break;

            case 'log':
                entry.log.push(msg.entry);
                logForChild(msg.entry.level, msg.entry.message);
                break;

            default:
                warn('Unknown message type, %s', msg.type);
                break;
        }
    });


    child.on('exit', function (code) {
        if (code !== 0) {
            info('Child terminated with code = ' + code);
            entry.finished_at = (new Date().toISOString());
            entry.status = 'failed';
            child.emit(
                'event', 'error',
                {
                    error: 'Child task process ' + req.task
                    + ' did not terminate cleanly. ('+code+')'
                });
                child.emit('event', 'finish', {});
                child.emit('finish');
        } else {
            info('Child terminated cleanly.');
        }
    });

    return child;
};


TaskRunner.prototype.reapChildren = function (req) {
    var self = this;
    for (var pid in self.children) {
        if (!self.children.hasOwnProperty(pid)) {
            continue;
        }
        self.children[pid].kill();
    }
};


module.exports = {
    TaskRunner: TaskRunner
};

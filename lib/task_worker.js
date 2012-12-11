var taskModule = process.argv[2];
var TaskClass = require(taskModule + '.js');
var bunyan = require('bunyan');

var logOpts = {
    name: taskModule,
    streams: [
        {
            path: process.env.logfile,
            level: 'debug'
        }
    ]
};

var log = bunyan.createLogger(logOpts);

var isString = function (obj) {
    return Object.prototype.toString.call(obj) === '[object String]';
};

log.info('task_worker started');

log.debug('Child ready to start, sending ready event to parent');
process.send({ type: 'ready' });

process.on('SIGTERM', function () {
    log.info('Task processes terminated. Exiting.');
    process.exit(0);
});

process.on('uncaughtException', function (err) {
    process.send({ type: 'exception', error: {
            message: err.message,
            stack: err.stack
        }
    });
    log.error('Uncaught exception in task child process: ');
    log.error(err.message);
    log.error(err);
    process.exit(1);
});


process.on('message', function (msg) {
    log.debug('Child received hydracp message from parent:');
    log.debug(msg);
    switch (msg.action) {
        case 'start':
            start(msg.req, msg.tasksPath);
            break;
        case 'subtask':
            var fn = task.subTaskCallbacks[msg.id];
            fn.apply(task, [msg.name, msg.event]);
            break;
        default:
            log.warn('Unknown task action, %s', msg.action);
            break;
    }
});

var task;

function start(req, tasksPath) {
    log.info('Instantiating ' + taskModule);
    log.info('task_id: ' + req.params.task_id);
    log.info('client_id: ' + req.params.client_id);
    task = new TaskClass(req);
    task.req = req;
    task.tasksPath = tasksPath;

    task.on('event', function (name, event) {
        log.info('Received event (' + name + ') from task instance:');
        log.debug(event);
        process.send({ type: 'event', name: name, event: event });
    });

    task.on('log', function (entry) {
        log[entry.level](entry.message);
        //    process.send({ type: 'log', entry: entry });
    });

    task.on('subtask', function (event) {
        log.info('Received a subtask event from task instance:');
        log.debug(event);
        process.send({
            type: 'subtask',
            resource: event.resource,
            task: event.task,
            msg: event.msg,
            id: event.id
        });
    });

    task.start();
}

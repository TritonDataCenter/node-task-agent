var EventEmitter = require('events').EventEmitter
  , util = require('util')
  , fs = require('fs')
  , path = require('path')
  , yaml = require('yaml')
  , JSV = require('JSV').JSV
  , smartdc_config = require('smartdc-config')
  , common = require('./common')
  , spawn = require('child_process').spawn;

var Task = module.exports = function (request) {
  EventEmitter.call(this);
  this.subTaskCallbacks = {};
  this.log = new Log(this);
}

util.inherits(Task, EventEmitter);

Task.prototype.validate = function (callback) {
  var self = this;

  // TODO preload and cache these files
 
  var schemaFilename = path.join(this.tasksPath, this.req.task, 'schema.yaml');
  fs.exists(schemaFilename, function (exists) {
    if (!exists) {
      return callback();
    }

    fs.readFile(schemaFilename, { encoding: 'utf8' }, function (error, data) {
      if (error) {
        throw new Error("error opening file: " + schemaFilename);
        return callback(error);
      }
      var schemaObject = yaml.eval(data.toString());
      var env = JSV.createEnvironment();
      var report = env.validate(self.req.params, schemaObject);
      if (report.errors.length) {
        return callback(new Error("Task parameters failed validation"), report.errors);
      }
      self.event('validate');
      return callback(null, report.errors);
    });
  });
}

var Log = function (task) {
  this.task = task;
  this.entries = [];
}

Log.LOG_LEVELS = { 'all': 0, 'debug': 1, 'info': 2, 'warn': 3 , 'error': 4 };

Log.prototype.dir = function (message) {
//   util.debug(util.inspect(message, null, Infinity));
  return this.logMessage('debug', message);
}

Log.prototype.debug = function (message) {
//   util.debug(message);
  return this.logMessage('debug', message);
}

Log.prototype.info = function (message) {
//   util.log(message);
  return this.logMessage('info', message);
}

Log.prototype.warn = function (message) {
//   util.error(message);
  return this.logMessage('warn', message);
}

Log.prototype.trace = function (message) {
//   util.error(message);
  return this.logMessage('trace', message);
}

Log.prototype.error = function (message) {
//   util.error(message);
  return this.logMessage('error', message);
}

Log.prototype.process = function (process, args, env, exitstatus, stdout, stderr) {
  var item
    = { timestamp:   new Date().toISOString()
      , level:       'debug'
      , process:     process
      , args:        args
      , env:         env
      , exitstatus:  exitstatus
      , stdout:      stdout
      , stderr:      stderr
      , type:        'process'
      };
  this.entries.push(item);
  this.task.emit('log', item);
  return item;
}

Log.prototype.logMessage = function (level, message, timestamp) {
  if (typeof timestamp === 'undefined') {
    timestamp = new Date().toISOString();
  }
  var item
    = { timestamp: timestamp
      , message:   message
      , level:     level
      , type:      'message'
      };
  this.entries.push(item);
  this.task.emit('log', item);
  return item;
}

Task.prototype.start = function (callback) {
  this.finish();
}

Task.prototype.finish = function (value, callback) {
  if (!value) 
    value = { log: this.log.entries }
  else 
    value.log = this.log.entries;
  this.event('finish', value);
}

Task.prototype.event = function (eventName, payload) {
  this.emit('event', eventName, payload ? payload : {});
}

Task.prototype.error = function (errorMsg, details) {
  var msg = { error: errorMsg };
  if (details) {
    msg.details = details;
  }
  this.event('error', msg);
}

Task.prototype.fatal = function (errorMsg, details) {
  var self = this;
  self.error(errorMsg, details);
  self.finish();
}

Task.prototype.progress = function (value) {
  this.event('progress', { value: value });
}

Task.prototype.run = function (binpath, args, env, callback) {
  var child = spawn(binpath, args, { encoding: 'utf8', env: env });

  var entry = this.log.process(binpath, args, env, undefined, "", "");
  child.stdout.on('data', function (data) {
    entry.stdout += data.toString();
  });

  child.stderr.on('data', function (data) {
    entry.stderr += data.toString();
  });

  child.on('exit', function (exitstatus) {
    entry.exitstatus = exitstatus;
    callback(exitstatus, entry.stdout, entry.stderr);
  });
}

Task.prototype.subTask = function (resource, task, msg, callback) {
  var id = common.genId();
  this.subTaskCallbacks[id] = callback;
  this.emit
    ( 'subtask'
    , { resource: resource
      , task: task
      , msg: msg
      , id: id
      }
    );
}

Task.createSteps = function (steps) {
  for (var k in steps) {
    var step = steps[k];
    var options
      = { description: step.description
        , progress:    step.progress
        };
    this.createStep(k, step.fn, options);
  }
}

/**
 * Use this "decorator" to ensure when step functions are called they emit
 * events on start/end of execution.
 *
 * Returns a function that emits an event when executed, and wraps the step
 * functions callback with a funcition that emits an 'end' event.
 */

Task.createStep = function (stepName, fn, options) {
  this.prototype[stepName] = function () {
    var self = this;

    self.emit
      ( 'event'
      , ['start', stepName].join(':')
      , { timestamp: (new Date).toISOString()
        , description: options.description
        }
      );

    /**
     * Partition the step function's received arguments into args and callback
     */
    var args = Array.prototype.slice.apply(arguments, [0, -1]);
    var stepCallback = Array.prototype.slice.call(arguments, -1)[0];

    args.push(function () {
      if (options.progress)
        self.progress(options.progress);

      self.emit
        ( 'event'
        , ['end', stepName].join(':')
        , { timestamp: (new Date).toISOString() }
        ); 
      return stepCallback.apply(this, arguments);
    });

    return fn.apply(this, args);
  };
}

Task.setStart = function (fn) {
  this.prototype.start = function () {
    var self = this;
    self.validate(function (error, report) {
      if (error) {
        self.error('Task parameters failed validation', report);
        self.finish();
        return;
      }
      smartdc_config.sdcConfig(function (error, config) {
        self.sdcConfig = config;
        self.progress(0);
        self.event('start', {});
        fn.apply(self, arguments);
      });
    });
  }
}

Task.setFinish = function (fn) {
  this.prototype.finish = function () {
    this.finish();
    fn.apply(this, arguments);
  }
}

Task.setValidate = function (fn) {
  this.prototype.validate = function () {
    this.event('task_validated', {});
    fn.apply(this, arguments);
  }
}

Task.createTask = function (task) {
  util.inherits(task, Task);
  task.setStart = Task.setStart;
  task.setFinish = Task.setFinish;
  task.createStep = Task.createStep;
  task.createSteps = Task.createSteps;
  task.setValidate = Task.setValidate;
}

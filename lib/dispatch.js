/*
 * This file includes code responsible for setting up the event handlers used
 * by the task child processes (task_worker).
 */

var TaskAgent = require('./task_agent');
var path = require('path');

function createTaskDispatchFn(agent) {
  return function (req) {
    var child = agent.runner.dispatch(req);
    setupChildEventHandlers(agent, child, req);
  };
}

function setupChildEventHandlers(agent, child, req) {
  child.on('finish', function () {
    req.finish();
  });

  child.on('progress', function (value) {
    req.progress(value);
  });

  child.on('event', function (eventName, event) {
    req.event(eventName, event);
  });

  child.on('subtask', function (id, resource, task, subtaskMessage) {
    function subtaskHandle(error, agentHandle) {
        agentHandle.sendTask(task, subtaskMessage, function (taskHandle) {
            taskHandle.on('event', function (eventName, msg) {
                var toSend = {
                    action: 'subtask',
                    id: id,
                    name: eventName,
                    event: msg
                };
                child.send(toSend);
            });
        });
    }

    agent.getLocalAgentHandle(resource, subtaskHandle);
  });
}

module.exports = {
    createTaskDispatchFn: createTaskDispatchFn
};

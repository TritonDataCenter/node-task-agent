#!/usr/bin/env node

var TaskAgent = require('../lib/task_agent');
var path = require('path');
var createTaskDispatchFn = require('../lib/dispatch').createTaskDispatchFn;
var createHttpTaskDispatchFn = require('../lib/dispatch').createHttpTaskDispatchFn;

var tasksPath = path.join(__dirname, '../tasks');

var options = {
    uuid: '123',
    reconnect: true,
    resource: 'task_agent',
    logname: 'task_agent',
    tasklogdir: '/var/log/task_agent'
};
var agent = new TaskAgent(options);

var queueDefns = [
    {
        name: 'demo_tasks',
        log: true,
        maxConcurrent: 4,
        tasks: [ 'demo' ],
        onmsg: createTaskDispatchFn(agent, tasksPath),
        onhttpmsg: createHttpTaskDispatchFn(agent, tasksPath)
    }
];

agent.configureAMQP(function () {
    agent.on('ready', function () {
        agent.setupQueues(queueDefns);
    });
    agent.start();
});

var testCase = require('nodeunit').testCase;
var restify = require('restify');
var Logger = require('bunyan');

var PROVISIONER_PORT = 5309;
var client;

function setup(cb) {
    client = restify.createJsonClient({
        agent: false,
        url: 'localhost'
    });
    cb();
}

function teardown(cb) {
    cb();
}

function testExecuteTaskHttp(test) {
    test.expect(0);
    client.get('/tasks', function (err, req, res, tasks) {
        test.deepEqual(tasks, [], 'there are no tasks');
        test.ok(res, 'got a response');
        test.equal(res.statusCode, 'GET /tasks returned 200');
        test.done();
    });
}

module.exports = {
    setUp: setup,
    tearDown: teardown,
    'execute a task via http': testExecuteTaskHttp
};

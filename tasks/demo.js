var Task = require('../lib/task')
  , util = require('util')

var DemoTask = module.exports = function (req) {
  Task.call(this);
  this.req = req;
  console.log("Created a DemoTask");
}

Task.createTask(DemoTask);

DemoTask.setStart(function (callback) {
  var self = this;
  console.log("Inside DemoTask#start");
  self.log.info("I'm logging a message");
  self.progress(50);
  self.finish({ hello: 'world' });
});


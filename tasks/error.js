/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var Task = require('../lib/task')
  , util = require('util')

var DemoTask = module.exports = function (req) {
  Task.call(this);
  this.req = req;
  console.log("Created a DemoTask");
}

util.inherits(DemoTask, Task);

Task.setValidate(DemoTask, function (callback) {
  callback(true);
});

Task.setStart(DemoTask, function (callback) {
  var self = this;
  console.log("Inside DemoTask#start");
  self.silly(1,2,function () {
    self.error("Failed inside DemoTask#start");
  });
});

Task.setFinish(DemoTask, function (callback) {
  console.log("Inside DemoTask#finish");
});

Task.createStep(DemoTask, 'silly', function (a,b,callback) {
  console.log("This is the silly step!");
  console.log(arguments);
  callback();
});

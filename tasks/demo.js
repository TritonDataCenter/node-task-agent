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

Task.createTask(DemoTask);

DemoTask.setStart(function (callback) {
  var self = this;
  console.log("Inside DemoTask#start");
  self.log.info("I'm logging a message");
  self.progress(50);
  self.finish({ hello: 'world' });
});


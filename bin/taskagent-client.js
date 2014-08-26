/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var Client = require('../lib/client')

var client = new Client();

var task = process.argv[2] || 'demo';

client.configureAMQP(function () {
  client.connect(function () {
    console.log("Connected!");
    client.getAgentHandle('demo', '564df8c3-26cb-30b4-e1a4-b9dd51680cf1', function (handle) {
      console.log("Got agent handle: " + handle.clientId);
      var msg = {};
      handle.sendTask(task, msg, function (taskHandle) {
        console.log("Inside the sendTask callback");
      });
    });
  });
});

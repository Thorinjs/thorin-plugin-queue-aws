'use strict';
const AWS = require('aws-sdk'),
  initQueue = require('./lib/queue.js');

/**
 * The queue system uses AWS SQS and exposes 3 functions:
 * - push(payload, _opt)
 * - pull(_opt)
 * - purge(_opt)
 *
 * Notes:
 * - Functions are promisified
 * */

module.exports = function (thorin, opt, pluginName) {
  const defaultOpt = {
    logger: pluginName || 'queue',
    url: null,  // the full queue URL
    options: null,  // additional AWS options
    messages: 1,  // max number of messages to pull once.
    wait: 20, // long-polling wait time in seconds
    visibility: 30,  // the number of seconds  the message is visible
    removeInvalid: true,   // automatically delete messages that are not valid (invalid JSON payload or format)
    delay: 0, // number of seconds to delay pushing
    aws: {
      signatureVersion: 'v4',   // AWS Signature version
      key: process.env.AWS_ACCESS_KEY || null,  // AWS Access Key
      secret: process.env.AWS_ACCESS_SECRET || null,  // AWS Secret Key
      region: process.env.AWS_REGION || null          // AWS Region
    }
  };
  let logger = thorin.logger(opt.logger);
  opt = thorin.util.extend(defaultOpt, opt);
  if (typeof opt.aws.accessKeyId === 'string') {
    opt.aws.key = opt.aws.accessKeyId;
    delete opt.aws.accessKeyId;
  }
  if (typeof opt.aws.secretAccessKey === 'string') {
    opt.aws.secret = opt.aws.secretAccessKey;
    delete opt.aws.secretAccessKey;
  }
  if (!opt.aws.key) {
    logger.warn(`No AWS Access key found`);
  }
  if (!opt.aws.secret) {
    logger.warn(`No AWS Secret Key found`);
  }
  if (opt.aws.key && opt.aws.secret) {
    opt.aws.secretAccessKey = opt.aws.secret;
    opt.aws.accessKeyId = opt.aws.key;
    delete opt.aws.secret;
    delete opt.aws.key;
  }

  let queueObj = initQueue(thorin, opt);
  queueObj.id = thorin.util.randomString(5);

  /**
   * CREATE a separate queue
   * */
  queueObj.create = (_opt) => {
    if (typeof _opt !== 'object' || !_opt) _opt = {};
    _opt = thorin.util.extend(opt, _opt);
    let qObj = initQueue(thorin, _opt);
    qObj.id = thorin.util.randomString(4);
    return qObj;
  };

  return queueObj;
};
module.exports.publicName = 'queue';
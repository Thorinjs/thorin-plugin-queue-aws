'use strict';
const AWS = require('aws-sdk'),
  initMessage = require('./message');

const SQS_MAX_BULK = 9;

/**
 * Created by Adrian on 22-Mar-17.
 */
module.exports = (thorin, opt) => {
  const async = thorin.util.async;
  const logger = thorin.logger(opt.logger);
  const queueObj = {};
  const awsOpt = Object.assign({}, opt.aws);
  const sqsObj = new AWS.SQS(awsOpt);
  const Message = initMessage(thorin, opt, sqsObj, parseError);

  /**
   * Starts pulling from the SQS queue with the specified options.
   * Resolves with a promise or callsback
   * */
  function doPull(options, done, _rawParams) {
    let params;
    if (_rawParams === true) {
      params = options;
    } else {
      params = getParams({
        MaxNumberOfMessages: opt.messages,
        QueueUrl: opt.url,
        VisibilityTimeout: opt.visibility,
        WaitTimeSeconds: opt.wait,
        MessageAttributeNames: ['All']
      }, options);
    }

    let isSingleMessage = (params.MaxNumberOfMessages === 1);
    sqsObj.receiveMessage(params, (err, data) => {
      if (err) {
        return done(parseError(err));
      }
      if (typeof data !== 'object' || !data) data = {};
      // IF we have no messages, we wait a bit and re-pull till we have something.
      if (!(data.Messages instanceof Array) || data.Messages.length === 0) {
        return redoPull(params, done);
      }
      let items = [];
      for (let i = 0, len = data.Messages.length; i < len; i++) {
        let msg = data.Messages[i],
          msgObj = new Message(msg, params);
        if (msgObj._valid()) {
          items.push(msgObj);
        } else if (opt.removeInvald) {
          msgObj.destroy((e) => {
            if (e) {
              logger.warn(`Failed to destroy invalid message: ${msgObj.id || 'unknown'}`);
              logger.debug(e);
              return;
            }
            logger.trace(`Destroyed invalid message: ${msgObj.id}`);
          });
        }
      }
      if (items.length === 0) {
        return redoPull(params, done);
      }
      if (isSingleMessage) {
        return done(null, items[0]);
      }
      return done(null, items);
    });
  }

  queueObj.pull = function PullMessage(options) {
    return promisifyFn(doPull, options, arguments);
  };

  /**
   * Performs a bulk push message to the current SQS message
   * Resolves with a promise or callback.
   * ARGUMENTS
   *  - items -> array[payload} => any kind of JSON-able values
   *  - options.delay (optional) - additional delay
   *  RESOLVES WITH
   *    - an array with failed messages.
   * */
  function doBulkPush(items, options, done, isSplit) {
    if (!(items instanceof Array) || items.length === 0) {
      if (!isSplit) {
        return done(thorin.error('SQS.INVALID', 'Push items are not present'));
      }
      return done();
    }
    let clean = [];
    for (let i = 0, len = items.length; i < len; i++) {
      try {
        let payload = JSON.stringify(items[i]);
        clean.push(payload);
      } catch (e) {
      }
    }
    if (clean.length === 0) {
      if (isSplit) {
        return done(thorin.error('SQS.INVALID', 'Push items are empty'));
      }
      return done();
    }
    let payloadParams = getParams({
      delay: opt.delay,
      url: opt.url
    }, options);

    let payload = {
      Entries: [],
      QueueUrl: payloadParams.url
    };
    let now = Date.now().toString(),
      result = {};
    for (let i = 0, len = clean.length; i < len; i++) {
      let item = clean[i],
        id = now + i.toString();
      result[id] = {
        sent: false
      };
      let entry = {
        DelaySeconds: payloadParams.delay,
        Id: id,
        MessageBody: item,
        MessageAttributes: {
          '_Timestamp': {
            DataType: 'String',
            StringValue: Date.now().toString()
          }
        }
      };
      payload.Entries.push(entry);
    }
    sqsObj.sendMessageBatch(payload, (e, data) => {
      if (e) {
        return done(parseError(e));
      }
      done(null, data.Failed || []);
    });
  }

  /**
   * Splits the bulk sending into multiple 9-item entries,
   * since AWS allows us to push 10 at a time.
   * */
  function splitBulkPush(items, options, done) {
    if (items.length <= SQS_MAX_BULK) {
      return doBulkPush(items, options, done, false);
    }
    let split = [];
    while (items.length > 0) {
      let tmp = items.splice(0, SQS_MAX_BULK);
      split.push(tmp);
    }
    let calls = [];
    split.forEach((sub) => {
      calls.push((fn) => doBulkPush(sub, options, fn, true));
    });
    async.series(calls, (e, res) => {
      if (e) return done(e);
      let failed = [];
      if (res instanceof Array) {
        for (let i = 0, len = res.length; i < len; i++) {
          if (res[i] instanceof Array) {
            failed = failed.concat(res[i]);
          }
        }
      }
      done(null, failed);
    });
  }

  /**
   * Push a message to the current SQS queue
   * Resolves with a promise or callsback
   * ARGUMENTS
   *  - payload -> any kind of JSON-able value
   *  - options.attributes (optional) key-value attributes for the message.
   *  - options.delay (optional) - additional delay
   * */
  function doPush(payload, options, done) {
    if (typeof payload === 'undefined' || payload == null) {
      return done(thorin.error('SQS.INVALID', 'Payload is not valid'));
    }
    try {
      payload = JSON.stringify(payload);
    } catch (e) {
      return done(thorin.error('SQS.INVALID', 'Payload cannot be converted to string'));
    }
    let params = getParams({
      delay: opt.delay,
      url: opt.url
    }, options);
    params = {
      DelaySeconds: params.delay,
      QueueUrl: params.url,
      MessageBody: payload,
      MessageAttributes: {
        '_Timestamp': {
          DataType: 'String',
          StringValue: Date.now().toString()
        }
      }
    };
    if (typeof options === 'object' && options) {
      if (typeof options.attributes === 'object' && options.attributes) {
        Object.keys(options.attributes).forEach((key) => {
          let val = options.attributes[key],
            item;
          if (typeof val === 'string') {
            item = {
              DataType: 'String',
              StringValue: val
            };
          } else if (typeof val === 'number') {
            item = {
              DataType: 'Number',
              StringValue: val.toString()
            }
          } else if (val instanceof Buffer) {
            item = {
              DataType: 'Binary',
              BinaryValue: val
            };
          }
          if (!item) return;
          params.MessageAttributes[key] = item;
        });
      }
    }
    sqsObj.sendMessage(params, (e, data) => {
      if (e) {
        return done(parseError(e));
      }
      done(null, data.MessageId || null);
    });
  }

  /**
   * BULK push multiple payload items to SQS
   * */
  queueObj.bulkPush = function BulkPushMessage(items, options) {
    let arg = Array.prototype.slice.call(arguments),
      fn = arg.pop();
    if (typeof fn === 'function') {
      splitBulkPush(items, options, fn);
      return;
    }
    return new Promise((resolve, reject) => {
      splitBulkPush(items, options, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  };

  /**
   * Push a single payload item to SQS
   * */
  queueObj.push = function PushMessage(payload, options) {
    let arg = Array.prototype.slice.call(arguments),
      fn = arg.pop();
    if (typeof fn === 'function') {
      doPush(payload, options, fn);
      return;
    }
    return new Promise((resolve, reject) => {
      doPush(payload, options, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  };

  /**
   * Purges the current queue, emptying it.
   * WARNING: this removes all pending messages
   * */
  function doPurge(options, done) {
    let params = getParams({
      url: opt.url
    }, options);
    params = {
      QueueUrl: params.url
    };
    sqsObj.purgeQueue(params, (e, data) => {
      if (e) return done(parseError(e));
      done();
    });
  }

  queueObj.purge = function PurgeMessage(options) {
    return promisifyFn(doPurge, options, arguments);
  };

  /**  PRIVATE FUNCTIONALITY */
  function redoPull(params, done) {
    let _wait = (params.WaitTimeSeconds === 0 ? 1 : params.WaitTimeSeconds);
    _wait = _wait * 1000;
    setTimeout(() => {
      doPull(params, done, true);
    }, _wait);
  }

  function getParams(params, options) {
    if (typeof opt.options === 'object' && opt.options) {
      params = thorin.util.extend(params, opt.options);
    }
    if (typeof options === 'object' && options) {
      params = thorin.util.extend(params, options);
    }
    return params;
  }

  function promisifyFn(callFn, options, arg) {
    arg = Array.prototype.slice.call(arg);
    let fn = arg.pop();
    if (typeof fn === 'function') {
      callFn(options, fn);
      return;
    }
    return new Promise((resolve, reject) => {
      callFn(options, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }


  /*
   * Parse the SQS Error as a standard thorin error
   * */
  function parseError(e) {
    let err = thorin.error(e.code || 'SQS.ERROR', e.message || 'An unexpected error occurred');
    err.ns = 'QUEUE';
    if (e.statusCode) {
      err.statusCode = e.statusCode;
    }
    if (e.requestId) err.requestId = e.requestId;
    if (typeof e.retryable === 'boolean') err.retryable = e.retryable;
    if (typeof e.retryDelay !== 'undefined') err.retryDelay = e.retryDelay;
    return err;
  }


  return queueObj;
};
'use strict';
/**
 * This is an instance of the Queue Message.
 * MESSAGE FIELDS AND ATTRIBUTES
 *
 * messageObj.id -> the SQS MessageID
 * messageObj.receipt -> The Receipt handler
 * messageObj.payload -> the payload we received
 * messageObj.ts -> the creation TimeStamp
 * messageObj.attributes -> any key-value attributes specified in push()
 */

module.exports = (thorin, opt, sqsObj, parseError) => {
  const logger = thorin.logger(opt.logger);

  const id = Symbol('id'),
    destroyed = Symbol('destroyed'),
    params = Symbol('params'),
    receipt = Symbol('receipt');


  class QueueMessage {

    constructor(data, _params) {
      if (typeof data !== 'object' || !data) return;
      this[params] = _params;
      this[destroyed] = false;
      if (typeof data.MessageId === 'string') {
        this[id] = data.MessageId;
      }
      if (typeof data.ReceiptHandle === 'string') {
        this[receipt] = data.ReceiptHandle;
      }
      if (typeof data.MessageAttributes === 'object' && data.MessageAttributes) {
        Object.keys(data.MessageAttributes).forEach((name) => {
          let item = data.MessageAttributes[name];
          if (name === '_Timestamp') {
            this.ts = parseInt(item.StringValue, 10);
            return;
          }
          let val = (item.DataType === 'String' ? item.StringValue : item.BinaryValue);
          if (typeof val === 'undefined' || val == null || val == '') return;
          if (!this.attributes) this.attributes = {};
          this.attributes[name] = val;
        });
      }
      this.payload = null;
      if (typeof data.Body === 'string' && data.Body) {
        try {
          this.payload = JSON.parse(data.Body);
        } catch (e) {
          this.payload = data.Body;
        }
      }
    }

    /**
     * Destroys the message using the message handler.
     * Internally, calls deleteMessage()
     * */
    destroy(fn) {
      if (typeof fn === 'function') {
        if (this[destroyed]) return fn();
        return doDestroy(this, fn);
      }
      if (this[destroyed]) return Promise.resolve();
      return new Promise((resolve, reject) => {
        doDestroy(this, (e) => {
          if (e) return reject(e);
          resolve();
        });
      });
    }

    get id() {
      return this[id] || null;
    }

    set id(v) {
    }

    get receipt() {
      return this[receipt] || null;
    }

    set receipt(v) {
    }

    _valid() {
      if (!this[id] || !this[receipt] || this.payload == null) return false;
      return true;
    }
  }

  function doDestroy(msgObj, done) {
    let _params = {
      QueueUrl: msgObj[params].QueueUrl,
      ReceiptHandle: msgObj[receipt]
    };
    sqsObj.deleteMessage(_params, (err, data) => {
      if (err) {
        return done(parseError(err));
      }
      msgObj[destroyed] = true;
      delete msgObj[params];
      msgObj[receipt] = null;
      msgObj.payload = null;
      done();
    });
  }

  return QueueMessage;
};

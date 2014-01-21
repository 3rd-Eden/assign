'use strict';

function noop() {
  /* You just wasted a second reading this comment, you're welcome */
}

/**
 * Data processor. Map/Reduce as promise like api. *buzzword*
 *
 * @constructor
 * @param {Mixed} context A reference to it self.
 * @param {Function} fn The callback function when data is completed.
 * @api private
 */
function Assignment(context, fn) {
  //
  // the `.and` allows for human readable chaining of code;
  // ```js
  // foo.bar().map().and.baz().reduce();
  // ```
  //
  this.fn = fn || noop;
  this.and = context;
  this.result = null;
  this.length = 0;
  this.rows = [];
  this.flow = [];
}

Assignment.prototype.__proto__ = require('stream').prototype;

/**
 * Start a map operation on the received data. This map operation will most
 * likely transform the row it received.
 *
 * ```js
 * assignment.map(function map(row) {
 *  return {
 *    id: row.id,
 *    hash: crypto.createHash('md5').update(row.data).digest('hex')
 *  }:
 * });
 * ```
 *
 * @param {Function} fn
 * @returns {This}
 * @api public
 */
Assignment.prototype.map = function map(fn) {
  if (!this.flow) return this;

  fn.assignment = 'map';
  this.flow.push(fn);

  return this;
};

/**
 * Reduce the results to a single value.
 *
 * @param {Function} fn
 * @returns {This}
 * @api public
 */
Assignment.prototype.reduce = function reduce(fn, initial) {
  if (!this.flow) return this;

  fn.assignment = 'reduce';
  this.flow.push(fn);

  if (arguments.length === 2) {
    this.result = initial;
  }

  return this;
};

/**
 * The emit allows you to split up the data in to multiple rows that will be
 * processed by the assignment flow.
 *
 * ```js
 * assignment.emit(function scan(row, emit) {
 *  if (row.foo) emit(row.foo);
 *  if (row.bar) emit(row.bar);
 *
 *  return false; // discard row.
 * });
 * ```
 *
 * @param {Function} fn
 * @returns {This}
 * @api public
 */
Assignment.prototype.emits = function emits(fn) {
  if (!this.flow) return this;

  fn.assignment = 'emit';
  this.flow.push(fn);

  return this;
};

/**
 * We've received a new chunk of data that we should process. If we don't
 * receive an `end` boolean we assume that we've received a chunk that needs to
 * processed instead.
 *
 * ```js
 * assignment.write([{}]);
 * assignment.write([{}], true);
 * ```
 *
 * @param {Mixed} data The data we need to consume and process.
 * @param {Boolean} end This was the last fragment of data we will receive.
 * @returns {Boolean}
 * @api private
 */
Assignment.prototype.write = function write(data, end) {
  if (!this.flow) return false;

  var assignment = this
    , row;

  data = Array.isArray(data) ? data : [data];

  /**
   * Iterate over the data structure.
   *
   * @param {Function} flow The current flow that needs to be executed.
   * @api private
   */
  function iterate(flow) {
    switch (flow.assignment) {
      case 'emit':
        if (flow(row, data.push.bind(data)) === false) {
          return false;
        }
      break;

      case 'reduce':
        assignment.result = flow(assignment.result, row, assignment.length);
      break;

      case 'map':
        row = flow(row, assignment.length);
      break;

      default:
        flow(row);
    }

    return true;
  }

  //
  // Iterate over the data, we need to remove items from the `data` array as the
  // `emit` method can add more items to the data feed.
  //
  while (row = data.shift()) {
    if (this.flow.every(iterate) && !this.result) {
      this.rows.push(row);
    }

    this.length++;
  }

  if (end === true) {
    this.fn(undefined, this.result || (this.length === 1 ? this.rows[0] : this.rows));
    this.destroy();
  }

  return true;
};

/**
 * End the assignment.
 *
 * @param {Mixed} data The data to consume.
 * @api private
 */
Assignment.prototype.end = function end(data) {
  return this.write(data, true);
};

/**
 * Once all operations are done, call this callback.
 *
 * @param {Function} fn The callback that is called once the assignment is done.
 * @returns {This}
 * @api public
 */
Assignment.prototype.finally = function final(fn) {
  this.fn = fn || this.fn;

  return this;
};

/**
 * Destroy the assignment. We're done with processing the data.
 *
 * @param {Error} err We're destroyed because we've received an error.
 * @api private
 */
Assignment.prototype.destroy = function destroy(err) {
  if (err) this.fn(err);

  this.and = this.flow = this.fn = this.rows = null;
};

//
// Expose the module.
//
module.exports = Assignment;

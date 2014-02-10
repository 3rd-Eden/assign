'use strict';

var fuse = require('fusing');

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
  if (!(this instanceof Assignment)) return new Assignment(context, fn);

  if ('function' === typeof context) {
    fn = context;
    context = null;
  }

  var writable = Assignment.predefine(this, Assignment.predefine.WRITABLE)
    , readable = Assignment.predefine(this);

  readable('and', context || this);
  writable('fn', fn || noop);
  writable('_async', false);
}

fuse(Assignment, require('stream'), {
  defaults: false
});

/**
 * The amount of rows we've processed so far.
 *
 * @type {Number}
 * @public
 */
Assignment.writable('length', 0);

/**
 * Stores the reduced result.
 *
 * @type {Mixed}
 * @private
 */
Assignment.writable('result', null);

/**
 * Reference to the rows we've or are processing.
 *
 * @type {Array}
 * @private
 */
Assignment.writable('rows', []);

/**
 * Our actual internal structure which contains the map/reduce/emit functions.
 *
 * @type {Array}
 * @private
 */
Assignment.writable('flow', []);

/**
 * Mark the next function that we're adding as an async processing function.
 *
 * @type {Assignment}
 * @public
 */
Assignment.writable('async', {
  get: function get() {
    this._async = true;
    return this;
  }
}, true);

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
Assignment.readable('map', function map(fn) {
  if (!this.flow) return this;

  fn.async = this._async;   // Should we do this async.
  fn.assignment = 'map';    // Process type.
  this.flow.push(fn);       // Store.
  this._async = false;      // Reset.

  return this;
});

/**
 * Reduce the results to a single value.
 *
 * @param {Function} fn
 * @returns {This}
 * @api public
 */
Assignment.readable('reduce', function reduce(fn, initial) {
  if (!this.flow) return this;

  fn.async = this._async;   // Should we do this async.
  fn.assignment = 'reduce'; // Process type.
  this.flow.push(fn);       // Store.
  this._async = false;      // Reset.

  if (arguments.length === 2) {
    this.result = initial;
  }

  return this;
});

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
Assignment.readable('emits', function emits(fn) {
  if (!this.flow) return this;

  fn.async = this._async;   // Should we do this async.
  fn.assignment = 'emits';  // Process type.
  this.flow.push(fn);       // Store.
  this._async = false;      // Reset.

  return this;
});

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
Assignment.readable('write', function write(data, end) {
  if (!this.flow) return false;

  var assignment = this
    , row;

  data = !Array.isArray(data)
    ? [data]
    : data;

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
});

/**
 * End the assignment.
 *
 * @param {Mixed} data The data to consume.
 * @api private
 */
Assignment.readable('end', function end(data) {
  return this.write(data, true);
});

/**
 * Once all operations are done, call this callback.
 *
 * @param {Function} fn The callback that is called once the assignment is done.
 * @returns {This}
 * @api public
 */
Assignment.readable('finally', function final(fn) {
  this.fn = fn || this.fn;

  return this;
});

/**
 * Destroy the assignment. We're done with processing the data.
 *
 * @param {Error} err We're destroyed because we've received an error.
 * @api private
 */
Assignment.readable('destroy', function destroy(err) {
  if (err) this.fn(err);

  this.and = this.flow = this.fn = this.rows = null;
});

//
// Expose the module.
//
module.exports = Assignment;

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

  var writable = Assignment.predefine(this, Assignment.predefine.WRITABLE);

  writable('and', context || this); // Chaining.
  writable('fn', fn || noop);       // Completion callback.
  writable('_async', false);        // Async processing indicator.
  writable('length', 0);            // The amount of rows we've processed so far.
  writable('result', null);         // Stores the reduced result.
  writable('rows', []);             // Reference to the processed data.
  writable('flow', []);             // Our internal flow/parse structure.
}

fuse(Assignment, require('stream'), {
  defaults: false
});

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
Assignment.readable('map', function setmap(fn, options) {
  var assign = this;

  if (!assign.flow) return assign;

  /**
   * Simple wrapper around the actual mapping function that processes the
   * content.
   *
   * @param {Mixed} row The data to process.
   * @param {Function} next Call the next flow.
   * @param {Function} done Fuck it, we're done.
   * @api private
   */
  function map(row, next, done) {
    if (!map.async) return next(undefined, fn(row, assign.length));

    fn(row, assign.length, next);
  }

  map.async = assign._async;  // Should we do this async.
  map.assignment = 'map';     // Process type.
  assign.flow.push(map);      // Store.
  assign._async = false;      // Reset.

  return assign;
});

/**
 * Reduce the results to a single value.
 *
 * @param {Function} fn
 * @returns {This}
 * @api public
 */
Assignment.readable('reduce', function setreduce(fn, initial) {
  var assign = this;

  if (!assign.flow) return assign;

  /**
   * Simple wrapper around the actual reducing function that processes the
   * content.
   *
   * @param {Mixed} row The data to process.
   * @param {Function} next Call the next flow.
   * @param {Function} done Fuck it, we're done.
   * @api private
   */
  function reduce(row, next, done) {
    if (!reduce.async) {
      assign.result = fn(assign.result, row, assign.length);
      return next();
    }

    fn(assign.result, row, assign.length, function processed(err, data) {
      assign.result = data;
      next(err);
    });
  }

  reduce.async = assign._async; // Should we do this async.
  reduce.assignment = 'reduce'; // Process type.
  assign.flow.push(fn);         // Store.
  assign._async = false;        // Reset.

  if (arguments.length === 2) {
    assign.result = initial;
  }

  return assign;
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
Assignment.readable('emits', function setemits(fn) {
  var assign = this;

  if (!assign.flow) return assign;

  /**
   * Push new data to the stack.
   *
   * @api private
   */
  function push() {
    assign.write([].slice.call(arguments, 0), { skip: 'emits' });
    return push;
  }

  /**
   * Simple wrapper around the actual mapping function that processes the
   * content.
   *
   * @param {Mixed} row The data to process.
   * @param {Function} next Call the next flow.
   * @param {Function} done Fuck it, we're done.
   * @api private
   */
  function emits(row, next, done) {
    if (!emits.async) {
      if (fn(row, push) === false) return done();
      return next();
    }

    fn(row, push, function done(err, moar) {
      if (err) return next(err);
      if (moar === false) return done();
    });
  }

  emits.async = assign._async; // Should we do this async.
  emits.assignment = 'emits';  // Process type.
  assign.flow.push(fn);        // Store.
  assign._async = false;       // Reset.

  return assign;
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

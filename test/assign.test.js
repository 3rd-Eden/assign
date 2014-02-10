describe('Assign', function () {
  'use strict';

  var Assignment = require('../')
    , chai = require('chai')
    , expect = chai.expect;

  it('is exported as a function', function () {
    expect(Assignment).to.be.a('function');
  });

  it('doenst require the new keyword to construct', function () {
    var assign = Assignment();

    expect(assign).to.be.instanceOf(Assignment);
  });

  describe('constructor', function () {
    var obj = {};
    var assign = new Assignment(obj);

    it('sets the context argument as `and`', function () {
      expect(assign.and).to.equal(obj);
    });

    it('makes `and` readonly', function () {
      try { assign.and = 1; }
      catch (e) { expect(e.message).to.contain('read only'); }

      expect(assign.and).to.equal(obj);
    });

    it('defaults to a `nope` function when none is provided', function () {
      expect(assign.fn).to.be.a('function');
    });
  });
});

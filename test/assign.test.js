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

    it('defaults to a `nope` function when none is provided', function () {
      expect(assign.fn).to.be.a('function');
    });
  });

  describe('.length', function () {
    it('indicates the amount of writes we\'ve received', function (done) {
      var assign = new Assignment(done);

      expect(assign.length).to.equal(0);
      assign.write(1);
      expect(assign.length).to.equal(1);
      assign.write(1);
      expect(assign.length).to.equal(2);
      assign.end();
    });
  });

  describe('#filter', function () {
    it('receives the written data', function (done) {
      var assign = new Assignment(function (err, data) {
        expect(data).to.be.a('array');
        expect(data).to.have.length(2);
        expect(data[0]).to.equal('foo');
        expect(data[1]).to.equal('foo');

        done(err);
      });

      assign.filter(function map(data) {
        return 'foo' === data;
      });

      assign.write('foo');
      assign.write('foo');
      assign.write('bar', {
        end: true
      });
    });

    it('allows multiple filter operations', function (done) {
      var assign = new Assignment(function (err, data) {
        expect(data).to.have.length(1);
        expect(data[0]).to.equal(true);

        done(err);
      });

      assign.filter(Boolean);
      assign.filter(function map(data) {
        return true === data;
      });

      assign.write(0);
      assign.write(1);
      assign.write(false);
      assign.write(true);
      assign.write(undefined);
      assign.write(null, {
        end: true
      });
    });
  });

  describe('#map', function () {
    it('receives the written data', function (done) {
      var assign = new Assignment(function (err, data) {
        expect(data).to.be.a('array');
        expect(data).to.have.length(2);
        expect(data[0]).to.equal('foo');
        expect(data[1]).to.equal('foo');

        done(err);
      });

      assign.map(function map(data) {
        expect(data.foo).to.include('bar');
        return 'foo';
      });

      assign.write({ foo: 'bar' });
      assign.write({ foo: 'barmitswa' }, {
        end: true
      });
    });

    it('allows multiple map operations', function (done) {
      var assign = new Assignment(function (err, data) {
        expect(data[0]).to.equal('bar');
        expect(data[1]).to.equal('bar');

        done(err);
      });

      assign.map(function map(data) {
        return 'foo';
      });
      assign.map(function map(data) {
        expect(data).to.equal('foo');
        return 'bar';
      });

      assign.write({ foo: 'bar' });
      assign.write({ foo: 'barmitswa' }, {
        end: true
      });
    });

    describe('.async', function () {
      it('processes the results async', function (done) {
        var assign = new Assignment(function (err, data) {
          expect(data[0]).to.equal('bar');
          done(err);
        });

        assign.async.map(function (data, index, next) {
          setTimeout(function () {
            next(undefined, data.foo);
          }, 10);
        });

        assign.write({ foo: 'bar'}, {
          end: true
        });
      });

      it('processes and combines data in order', function (done) {
        var assign = new Assignment(function (err, data) {
          expect(data[0]).to.equal(0);
          expect(data[1]).to.equal(1);

          done(err);
        });

        assign.async.map(function (data, index, next) {
          if (1 === index) return setTimeout(function () {
            next(undefined, index);
          }, 100);

          next(undefined, index);
        });

        assign.write('foo');
        assign.write({foo: 'bar' }, {
          end: true
        });
      });
    });
  });
});

'use strict';

const _ = require('lodash');
const fakeReader = require('./lib/fake_reader.js');
const {
    Stream, StreamEntity, isStream, isStreamEntity
} = require('../');

describe('Stream', () => {
    describe('when constructed with a reader', () => {
        let sut;
        const batchSize = 100;

        beforeEach(() => {
            sut = new Stream(fakeReader(batchSize));
        });

        it('should be a stream', () => {
            expect(isStream(sut)).toBeTrue();
        });

        describe('->done', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.done(_.noop))).toBeTrue();
            });
            describe('when the stream errors', () => {
                beforeEach(() => {
                    let count = 0;
                    const failAt = _.random(1, batchSize);
                    sut.eachAsync((msg, next) => {
                        count += 1;
                        if (failAt === count) {
                            next(new Error('Uh oh'));
                        } else {
                            next();
                        }
                    });
                });
                it('should yield an error', (done) => {
                    sut.done((err) => {
                        expect(_.toString(err)).toEqual('Error: Uh oh');
                        done();
                    });
                });
            });
            it('when successful', (done) => {
                sut.done((err, results) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    expect(results).toBeUndefined();
                    done();
                });
            });
        });

        describe('->each', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.each(_.noop))).toBeTrue();
            });

            describe('when successful', () => {
                const records = [];

                beforeEach((done) => {
                    records.length = 0;
                    sut.each((record) => {
                        records.push(record);
                    });
                    sut.done(done);
                });

                afterEach(() => {
                    records.length = 0;
                });

                it('should collect each item in the batch', () => {
                    expect(records).toBeArrayOfSize(batchSize);
                });

                it('should each record should a stream entity', () => {
                    _.each(records, (record) => {
                        expect(record).toEqual(jasmine.any(StreamEntity));
                        expect(isStreamEntity(record)).toBeTrue();
                    });
                });
            });
        });

        describe('->filter', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.filter(_.noop))).toBeTrue();
            });

            describe('when successful', () => {
                const records = [];
                beforeEach((done) => {
                    records.length = 0;
                    let count = 0;
                    sut.filter(() => {
                        count += 1;
                        return count % 2 === 0;
                    });
                    sut.each((record) => {
                        records.push(record);
                    });
                    sut.done(done);
                });
                afterEach(() => {
                    records.length = 0;
                });

                it('should collect half of the items in the batch', () => {
                    expect(records).toBeArrayOfSize(batchSize / 2);
                });
                it('should each record should a stream entity', () => {
                    _.each(records, (record) => {
                        expect(record).toEqual(jasmine.any(StreamEntity));
                        expect(isStreamEntity(record)).toBeTrue();
                    });
                });
            });
        });

        describe('->eachAsync', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.eachAsync(_.noop))).toBeTrue();
            });
            describe('when handling errors', () => {
                it('should the stream should fail', (done) => {
                    sut.eachAsync((record, next) => {
                        setImmediate(() => {
                            next(new Error('Uh oh'));
                        });
                    });
                    sut.done((err) => {
                        expect(err.toString()).toEqual('Error: Uh oh');
                        done();
                    });
                });
            });
            describe('when paused mid-stream', () => {
                it('should the stream still have the same result count', (done) => {
                    const pauseAfter = _.after(_.random(1, batchSize), _.once(() => {
                        sut.pause();
                        _.delay(() => {
                            sut.resume();
                        }, 10);
                    }));
                    const records = [];
                    sut.eachAsync((record, next) => {
                        expect(sut.isPaused()).toBeFalse();
                        records.push(record);
                        setImmediate(() => {
                            pauseAfter();
                            next(null);
                        });
                    });
                    sut.done((err) => {
                        expect(err).toBeFalsy();
                        expect(records).toBeArrayOfSize(batchSize);
                        done();
                    });
                }, 5000);
            });
            describe('when successful', () => {
                const records = [];
                beforeEach((done) => {
                    records.length = 0;
                    sut.eachAsync((record, next) => {
                        setImmediate(() => {
                            records.push(record);
                            next();
                        });
                    });
                    sut.done(done);
                });
                afterEach(() => {
                    records.length = 0;
                });

                it('should collect each item in the batch', () => {
                    expect(records).toBeArrayOfSize(batchSize);
                });
                it('should each record should a stream entity', () => {
                    _.each(records, (record) => {
                        expect(record).toEqual(jasmine.any(StreamEntity));
                        expect(isStreamEntity(record)).toBeTrue();
                    });
                });
            });
        });

        describe('->map', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.map(_.noop))).toBeTrue();
            });

            describe('when successful', () => {
                const records = [];
                beforeEach((done) => {
                    records.length = 0;
                    sut.map((record) => {
                        record.processed = true;
                        return record;
                    });
                    sut.each((record) => {
                        records.push(record);
                    });
                    sut.done(done);
                });
                afterEach(() => {
                    records.length = 0;
                });

                it('should collect each item in the batch', () => {
                    expect(records).toBeArrayOfSize(batchSize);
                });

                it('should each record should a stream entity', () => {
                    _.each(records, (record) => {
                        expect(record).toEqual(jasmine.any(StreamEntity));
                        expect(isStreamEntity(record)).toBeTrue();
                    });
                });

                it('should have the the records with the properly new field', () => {
                    _.forEach(records, (record) => {
                        expect(record.processed).toBeTrue();
                    });
                });
            });
        });

        describe('->mapAsync', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.mapAsync(_.noop))).toBeTrue();
            });
            describe('when handling errors', () => {
                it('should the stream should fail', (done) => {
                    sut.mapAsync((record, next) => {
                        setImmediate(() => {
                            next(new Error('Uh oh'));
                        });
                    });
                    sut.done((err) => {
                        expect(err.toString()).toEqual('Error: Uh oh');
                        done();
                    });
                });
            });
            describe('when paused mid-stream', () => {
                it('should the stream still have the same result count', (done) => {
                    const pauseAfter = _.after(_.random(1, batchSize), _.once(() => {
                        sut.pause();
                        _.delay(() => {
                            sut.resume();
                        }, 10);
                    }));
                    const records = [];
                    sut.mapAsync((record, next) => {
                        expect(sut.isPaused()).toBeFalse();
                        records.push(record);
                        setImmediate(() => {
                            pauseAfter();
                            next(null, record);
                        });
                    });
                    sut.done((err) => {
                        expect(err).toBeFalsy();
                        expect(records).toBeArrayOfSize(batchSize);
                        done();
                    });
                });
            });
            describe('when successful', () => {
                const records = [];
                beforeEach((done) => {
                    records.length = 0;
                    sut.mapAsync((record, next) => {
                        setImmediate(() => {
                            record.processed = true;
                            next(null, record);
                        });
                    });
                    sut.each((record) => {
                        records.push(record);
                    });
                    sut.done(done);
                });
                afterEach(() => {
                    records.length = 0;
                });

                it('should collect each item in the batch', () => {
                    expect(records).toBeArrayOfSize(batchSize);
                });
                it('should each record should a stream entity', () => {
                    _.each(records, (record) => {
                        expect(record).toEqual(jasmine.any(StreamEntity));
                        expect(isStreamEntity(record)).toBeTrue();
                    });
                });
                it('should have the the records with the properly new field', () => {
                    _.forEach(records, (record) => {
                        expect(record.processed).toBeTrue();
                    });
                });
            });
        });

        describe('->toArray', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.toArray(_.noop))).toBeTrue();
            });
            describe('when the stream errors', () => {
                beforeEach(() => {
                    let count = 0;
                    const failAt = _.random(1, batchSize);
                    sut.eachAsync((msg, next) => {
                        count += 1;
                        if (failAt === count) {
                            next(new Error('Uh oh'));
                        } else {
                            next();
                        }
                    });
                });
                it('should yield an error', (done) => {
                    sut.toArray((err) => {
                        expect(_.toString(err)).toEqual('Error: Uh oh');
                        done();
                    });
                });
            });
            describe('when successful', () => {
                let records = [];
                beforeEach((done) => {
                    sut.toArray((err, _records) => {
                        records = _records;
                        done(err);
                    });
                });

                it('should collect each item in the batch', () => {
                    expect(records).toBeArrayOfSize(batchSize);
                });
                it('should each record should a stream entity', () => {
                    _.each(records, (record) => {
                        expect(record).toEqual(jasmine.any(StreamEntity));
                        expect(isStreamEntity(record)).toBeTrue();
                    });
                });
            });
        });
    });
});

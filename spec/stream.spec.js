'use strict';

const _ = require('lodash');
const Promise = require('bluebird');

const fakeStream = require('./lib/fake_stream.js');
const {
    Stream, StreamEntity, isStream, isStreamEntity
} = require('../');

describe('Stream', () => {
    let sut;
    const batchSize = 100;

    beforeEach(() => {
        sut = fakeStream(batchSize);
    });

    afterEach(() => {
        sut.destroy();
        sut = null;
    });

    it('should be a stream', () => {
        expect(isStream(sut)).toBeTrue();
    });

    describe('->stats', () => {
        it('should start with values of zeros', () => {
            expect(sut.stats()).toEqual({
                processed: 0,
                running: 0,
                pending: 0,
            });
        });

        it('should increment with each write', (done) => {
            const stream = new Stream();
            const after = _.after(100, () => {
                stream.end();
            });
            _.times(100, (i) => {
                const count = i + 1;
                stream.write({ count }).then((result) => {
                    expect(result).toBeTruthy();
                    after();
                }).catch(done.fail);
            });
            stream.each(r => r);
            stream.done().then(() => {
                expect(stream.stats()).toEqual({
                    processed: 100,
                    running: 0,
                    pending: 0,
                });
                done();
            }).catch(done.fail);
        });
    });

    describe('->end', () => {
        it('should not be ended before it begins', () => {
            expect(sut.isEnded()).toBeFalse();
        });

        it('should be ended after writing 100 records', (done) => {
            const stream = new Stream();
            stream.resume();
            const after = _.after(100, () => {
                stream.end();
                expect(stream.isEnded()).toBeFalse();
            });
            expect(stream.isEnded()).toBeFalse();
            _.times(100, (i) => {
                stream.write({ i }).then((result) => {
                    expect(result).toBeTruthy();
                    after();
                }).catch(done.fail);
            });
            stream.done().then(() => {
                expect(stream.isEnded()).toBeTrue();
                done();
            }).catch(done.fail);
        });
    });

    describe('->pause', () => {
        it('should be paused before it begins', () => {
            expect(sut.isPaused()).toBeTrue();
            sut.write({ i: 1 });
        });

        it('should be paused after writing 100 records', (done) => {
            sut.resume();
            sut.write({ i: 1 }).then((firstResult) => {
                expect(sut.isPaused()).toBeFalse();
                expect(firstResult).toBeTruthy();
            }).then(() => {
                const after = _.after(99, () => {
                    sut.pause();
                    expect(sut.isPaused()).toBeTrue();
                    done();
                });
                _.times(99, (i) => {
                    sut.write({ i }).then((result) => {
                        expect(result).toBeTruthy();
                        after();
                    }).catch(done.fail);
                });
            }).catch(done.fail);
        });

        it('should be paused after pausing', (done) => {
            sut.resume();
            const after = _.after(50, _.once(() => {
                sut.pause();
                expect(sut.isPaused()).toBeTrue();
                done();
            }));
            _.times(50, (i) => {
                sut.write({ i }).then((result) => {
                    if (i === 1) {
                        expect(sut.isPaused()).toBeFalse();
                    }
                    expect(result).toBeTruthy();
                    after();
                }).catch(done.fail);
            });
        });
    });

    describe('->write', () => {
        it('should return a promise', () => {
            expect(sut.write({ hello: 'hi' }) instanceof Promise).toBe(true);
        });

        describe('when writing null', () => {
            it('should yield an error', (done) => {
                sut.write(null).then(done.fail).catch((err) => {
                    expect(err.message).toEqual('Cannot write empty data to stream');
                    done();
                });
            });
        });

        describe('when writing an object', () => {
            it('should return a stream entity', (done) => {
                sut.write({ hello: 'hi' }).then((result) => {
                    expect(isStreamEntity(result)).toBeTrue();
                    done();
                }).catch(done.fail);
            });
        });

        describe('when writing an string', () => {
            it('should return a stream entity', (done) => {
                sut.write('hello').then((result) => {
                    expect(isStreamEntity(result)).toBeTrue();
                    done();
                }).catch(done.fail);
            });
        });

        describe('when writing an error', () => {
            it('should cause the stream to emit an error', (done) => {
                sut.write('hello').catch(done.fail);
                setImmediate(() => {
                    sut.write(new Error('Uh oh')).catch(done.fail);
                });
                sut.done().then(done.fail)
                    .catch((err) => {
                        expect(err.message).toEqual('Uh oh');
                        done();
                    });
            });
        });

        describe('when the stream is paused', () => {
            it('should write the message', (done) => {
                sut.write({ hello: 'hi' }).then(() => {
                    done();
                }).catch(done.fail);
            });
        });

        it('should not fail if given an array of Stream Entities', (done) => {
            const stream = new Stream();
            const input = [new StreamEntity({ id: 'hello' }), new StreamEntity({ id: 'hi' })];
            stream.write(input).catch(done.fail).then((results) => {
                expect(results[0].toJSON().id).toEqual('hello');
                expect(results[1].toJSON().id).toEqual('hi');
                stream.end();
            });
            stream.toArray().then((results) => {
                expect(results[0].toJSON().id).toEqual('hello');
                expect(results[1].toJSON().id).toEqual('hi');
                done();
            }).catch(done.fail);
            expect(isStream(stream)).toBeTrue();
        });

        it('should fail if given an array', (done) => {
            const input = ['hi', 'hello'];
            sut.write(input).then(done.fail).catch((err) => {
                expect(err.message).toEqual('Invalid input to Stream->write()');
                done();
            });
        });
    });

    describe('->done', () => {
        it('should return a promise', () => {
            expect(sut.done() instanceof Promise).toBe(true);
        });
        describe('when the stream errors', () => {
            beforeEach(() => {
                let count = 0;
                const failAt = _.random(1, batchSize);
                sut.each((msg) => {
                    count += 1;
                    if (failAt === count) {
                        return Promise.reject(new Error('Uh oh'));
                    }
                    return Promise.resolve(msg);
                });
            });
            it('should reject with an error', (done) => {
                sut.done().then(() => done.fail()).catch((err) => {
                    expect(err.toString()).toEqual('Error: Uh oh');
                    done();
                });
            });
        });
        it('when successful', (done) => {
            sut.done().then((results) => {
                expect(results == null).toBe(true);
                done();
            }).catch(done.fail);
        });
    });

    describe('->each', () => {
        it('should return a teraslice stream', () => {
            expect(isStream(sut.each())).toBeTrue();
        });

        describe('when doing sync operations', () => {
            const records = [];

            beforeEach(() => {
                records.length = 0;
                return sut.each((record) => {
                    records.push(record);
                }).done();
            });

            afterEach(() => {
                records.length = 0;
            });

            it('should collect each item in the batch', () => {
                expect(_.size(records)).toEqual(batchSize);
            });

            it('should each record should a stream entity', () => {
                _.each(records, (record) => {
                    expect(record).toEqual(jasmine.any(StreamEntity));
                    expect(isStreamEntity(record)).toBeTrue();
                });
            });
        });
        describe('when handling errors', () => {
            it('should the stream should fail', (done) => {
                sut.each(() => Promise.delay(0).then(() => Promise.reject(new Error('Uh oh'))));
                sut.done().then(() => done.fail()).catch((err) => {
                    expect(err.toString()).toEqual('Error: Uh oh');
                    done();
                });
            });
        });
        describe('when paused mid-stream', () => {
            it('should the stream still have the same result count', (done) => {
                const records = [];
                sut.each((record) => {
                    expect(sut.isPaused()).toBeFalsy();
                    records.push(record);
                    return Promise.delay(0).then(() => {
                        if (_.size(records) === 10) {
                            sut.pause();
                            return Promise.delay(10).then(() => sut.resume());
                        }
                        return Promise.resolve();
                    });
                });
                sut.done().then(() => {
                    expect(_.size(records)).toEqual(batchSize);
                    done();
                }).catch(done.fail);
            }, 5000);
        });
        describe('when successful', () => {
            const records = [];
            beforeEach(() => {
                records.length = 0;
                sut.each(record => Promise.delay(0).then(() => {
                    records.push(record);
                }));
                return sut.done();
            });

            it('should collect each item in the batch', () => {
                expect(_.size(records)).toEqual(batchSize);
            });
            it('should each record should a stream entity', () => {
                _.each(records, (record) => {
                    expect(record).toEqual(jasmine.any(StreamEntity));
                    expect(isStreamEntity(record)).toBeTrue();
                });
            });
        });

        it('should work when chained with done', () => sut.each(() => Promise.delay(0)).done());

        it('should work when chained with toArray', () => sut.each(() => Promise.delay(0)).toArray((results) => {
            expect(results).toBeArrayOfSize(0);
        }));
    });

    describe('->filter', () => {
        it('should return a teraslice stream', () => {
            expect(isStream(sut.filter(_.noop))).toBeTrue();
        });

        describe('when successful', () => {
            const records = [];
            beforeEach(() => {
                records.length = 0;
                let count = 0;
                return sut.filter(() => {
                    count += 1;
                    return count % 2 === 0;
                }).each((record) => {
                    records.push(record);
                }).done();
            });
            afterEach(() => {
                records.length = 0;
            });

            it('should collect half of the items in the batch', () => {
                expect(_.size(records)).toEqual(batchSize / 2);
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

        describe('when doing sync operations', () => {
            const records = [];
            beforeEach(() => {
                records.length = 0;
                sut.map((record) => {
                    record.processed = true;
                    return record;
                });
                sut.each((record) => {
                    records.push(record);
                });
                return sut.done();
            });
            afterEach(() => {
                records.length = 0;
            });

            it('should collect each item in the batch', () => {
                expect(_.size(records)).toEqual(batchSize);
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
        describe('when handling errors', () => {
            it('should the stream should fail', (done) => {
                sut.map(() => Promise.reject(new Error('Uh oh')));
                sut.done().then(() => done.fail()).catch((err) => {
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
                expect(sut.isPaused()).toBeTruthy();
                sut.map((record) => {
                    records.push(record);
                    pauseAfter();
                    return record;
                });
                sut.done().then(() => {
                    expect(sut.isPaused()).toBeFalsy();
                    expect(_.size(records)).toEqual(batchSize);
                    done();
                }).catch(done.fail);
            });
        });
        describe('when doing async operations', () => {
            const records = [];
            beforeEach(() => {
                records.length = 0;
                sut.map(record => Promise.delay(0).then(() => {
                    record.processed = true;
                    return record;
                }));
                sut.each((record) => {
                    records.push(record);
                });
                return sut.done();
            });

            it('should collect each item in the batch', () => {
                expect(_.size(records)).toEqual(batchSize);
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
        it('should work when chained with done', () => sut.map(record => Promise.delay(0).then(() => record)).done());

        it('should work when chained with toArray', () => sut.map(record => Promise.delay(0).then(() => record)).toArray().then((results) => {
            expect(results).toBeArrayOfSize(batchSize);
        }));
    });

    describe('->toArray', () => {
        it('should return a promise', () => {
            expect(sut.toArray() instanceof Promise).toBe(true);
        });
        describe('when the stream errors', () => {
            beforeEach(() => {
                let count = 0;
                const failAt = _.random(1, batchSize);
                sut.each(() => {
                    count += 1;
                    if (failAt === count) {
                        return Promise.reject(new Error('Uh oh'));
                    }
                    return Promise.resolve();
                });
            });
            it('should reject with an error', (done) => {
                sut.toArray().then(() => done.fail()).catch((err) => {
                    expect(err.toString()).toEqual('Error: Uh oh');
                    done();
                });
            });
        });
        describe('when successful', () => {
            let records;
            beforeEach(() => sut.each(() => {}).toArray().then((_records) => {
                records = _records;
            }));

            it('should collect each item in the batch', () => {
                expect(_.size(records)).toEqual(batchSize);
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

'use strict';

const _ = require('lodash');
const { promisify } = require('util');

const setImmediatePromise = promisify(setImmediate);
const setTimeoutPromise = promisify(setTimeout);
const fakeStream = require('./lib/fake_stream.js');
const {
    StreamEntity, isStream, isStreamEntity
} = require('../');

describe('Stream', () => {
    describe('when constructed with a reader', () => {
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
            it('when successful', async () => {
                const results = await sut.done();
                expect(results == null).toBe(true);
            });
        });

        describe('->each', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.each())).toBeTrue();
            });

            describe('when successful', () => {
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

        describe('->each async', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.each(_.noop))).toBeTrue();
            });
            describe('when handling errors', () => {
                it('should the stream should fail', (done) => {
                    sut.each(async () => {
                        await setImmediatePromise();
                        return Promise.reject(new Error('Uh oh'));
                    });
                    sut.done().then(() => done.fail()).catch((err) => {
                        expect(err.toString()).toEqual('Error: Uh oh');
                        done();
                    });
                });
            });
            describe('when paused mid-stream', () => {
                it('should the stream still have the same result count', async () => {
                    const records = [];
                    sut.each(async (record) => {
                        expect(sut.isPaused()).toBeFalsy();
                        records.push(record);
                        await setImmediatePromise();
                        if (_.size(records) === 10) {
                            sut.pause();
                            await setTimeoutPromise(10);
                            sut.resume();
                        }
                    });
                    await sut.done();
                    expect(_.size(records)).toEqual(batchSize);
                }, 5000);
            });
            describe('when successful', () => {
                const records = [];
                beforeEach(() => {
                    records.length = 0;
                    sut.each(async (record) => {
                        await setImmediatePromise();
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
            });

            it('should work when chained with done', () => sut.each(() => setImmediatePromise()).done());

            it('should work when chained with toArray', () => sut.each(() => setImmediatePromise()).toArray((results) => {
                expect(results).toBeArrayOfSize(0);
            }));
        });

        describe('->map', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.map(_.noop))).toBeTrue();
            });

            describe('when successful', () => {
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
        });

        describe('->map async', () => {
            it('should return a teraslice stream', () => {
                expect(isStream(sut.map(_.noop))).toBeTrue();
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
                it('should the stream still have the same result count', async () => {
                    const pauseAfter = _.after(_.random(1, batchSize), _.once(() => {
                        sut.pause();
                        _.delay(() => {
                            sut.resume();
                        }, 10);
                    }));
                    const records = [];
                    expect(sut.isPaused()).toBeFalsy();
                    sut.map(async (record) => {
                        records.push(record);
                        pauseAfter();
                        return record;
                    });
                    await sut.done();
                    expect(_.size(records)).toEqual(batchSize);
                });
            });
            describe('when successful', () => {
                const records = [];
                beforeEach(() => {
                    records.length = 0;
                    sut.map(async (record) => {
                        await setImmediatePromise();
                        record.processed = true;
                        return record;
                    });
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
            it('should work when chained with done', async () => {
                sut.map(async (record) => {
                    await setImmediatePromise();
                    return record;
                }).done();
            });

            it('should work when chained with toArray', async () => {
                const results = await sut.map(async (record) => {
                    await setImmediatePromise();
                    return record;
                }).toArray();
                expect(results).toBeArrayOfSize(batchSize);
            });
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
                beforeEach(() => sut.toArray().then((_records) => {
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
});

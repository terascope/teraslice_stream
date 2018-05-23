'use strict';

const _ = require('lodash');
const FakeReader = require('./lib/fake_reader.js');
const Stream = require('../');
const StreamEntity = require('../').StreamEntity;

describe('Steam', () => {
    describe('when constructed with a reader', () => {
        let sut;
        let reader;
        const batchSize = 100;

        beforeEach(() => {
            reader = new FakeReader(batchSize);
            sut = new Stream(reader);
        });

        describe('->each', () => {
            const records = [];
            beforeEach((done) => {
                records.length = 0;
                sut.each((record) => {
                    records.push(record);
                });
                sut.done((err) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    done();
                });
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
                });
            });
        });

        describe('->eachAsync', () => {
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
                    sut.done((err) => {
                        if (err) {
                            done(err);
                            return;
                        }
                        done();
                    });
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
                    });
                });
            });
        });

        describe('->map', () => {
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
                sut.done((err) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    done();
                });
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
                });
            });

            it('should have the the records with the properly new field', () => {
                _.forEach(records, (record) => {
                    expect(record.processed).toBeTrue();
                });
            });
        });

        describe('->mapAsync', () => {
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
                    sut.done((err) => {
                        if (err) {
                            done(err);
                            return;
                        }
                        done();
                    });
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
                    });
                });
                it('should have the the records with the properly new field', () => {
                    _.forEach(records, (record) => {
                        expect(record.processed).toBeTrue();
                    });
                });
            });
        });
    });
});


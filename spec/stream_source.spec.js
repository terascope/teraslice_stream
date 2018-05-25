'use strict';

const _ = require('lodash');
const {
    StreamSource, isStreamSource, isStreamEntity, StreamEntity
} = require('../');

describe('StreamSource', () => {
    describe('when constructed', () => {
        it('should return a stream source', () => {
            const sut = new StreamSource();
            expect(isStreamSource(sut)).toBeTrue();
            sut.destroy();
        });

        it('should not fail if given an array of Stream Entity', (done) => {
            const input = [new StreamEntity({ id: 'hello' }), new StreamEntity({ id: 'hi' })];
            const sut = new StreamSource(input);
            sut.toStream().toArray((err, results) => {
                if (err) {
                    done(err);
                    return;
                }
                expect(results[0].toJSON().id).toEqual('hello');
                expect(results[1].toJSON().id).toEqual('hi');
                sut.destroy();
                done();
            });
            expect(isStreamSource(sut)).toBeTrue();
        });

        it('should fail if given an array', () => {
            const input = ['hi', 'hello'];
            expect(() => new StreamSource(input)).toThrowError('StreamSource requires an array of StreamEntities as input');
        });
    });

    describe('->count', () => {
        let sut;
        beforeEach(() => {
            sut = new StreamSource();
            sut.resume();
        });

        afterEach(() => {
            sut.destroy();
        });

        it('should start with a count of zero', () => {
            expect(sut.count).toEqual(0);
        });

        it('should increment with each write', (done) => {
            const after = _.after(100, done);
            _.times(100, (i) => {
                const count = i + 1;
                sut.write({ count }, (err, result) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    expect(result).toBeTruthy();
                    after();
                });
                expect(sut.count).toEqual(count);
            });
        });
    });

    describe('->end', () => {
        let sut;
        beforeEach(() => {
            sut = new StreamSource();
        });

        afterEach(() => {
            sut.destroy();
        });

        it('should not be ended before it begins', () => {
            expect(sut.isEnded()).toBeFalse();
        });

        it('should be ended after writing 100 records', (done) => {
            sut.resume();
            const after = _.after(100, () => {
                sut.end();
                expect(sut.isEnded()).toBeTrue();
                done();
            });
            expect(sut.isEnded()).toBeFalse();
            _.times(100, (i) => {
                sut.write({ i }, (err, result) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    expect(result).toBeTruthy();
                    after();
                });
            });
        });
    });

    describe('->pause', () => {
        let sut;
        beforeEach(() => {
            sut = new StreamSource();
        });

        afterEach(() => {
            sut.destroy();
        });

        it('should be paused before it begins', () => {
            expect(sut.isPaused()).toBeTrue();
            sut.write({ i: 1 }, _.noop);
        });

        it('should be paused after writing 100 records', (done) => {
            const after = _.after(99, () => {
                sut.end();
                expect(sut.isPaused()).toBeTrue();
                done();
            });
            sut.resume();
            sut.write({ i: 1 }, (err, firstResult) => {
                if (err) {
                    done(err);
                    return;
                }
                expect(sut.isPaused()).toBeFalse();
                expect(firstResult).toBeTruthy();
                _.times(99, (i) => {
                    sut.write({ i }, (err, result) => {
                        if (err) {
                            done(err);
                            return;
                        }
                        expect(result).toBeTruthy();
                        after();
                    });
                });
            });
        });

        it('should be paused after pausing', (done) => {
            sut.resume();
            const after = _.after(50, _.once(() => {
                sut.pause();
                expect(sut.isPaused()).toBeTrue();
                done();
            }));
            _.times(50, (i) => {
                sut.write({ i }, (err, result) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    if (i === 1) {
                        expect(sut.isPaused()).toBeFalse();
                    }
                    expect(result).toBeTruthy();
                    after();
                });
            });
        });
    });

    describe('->write', () => {
        let sut;
        beforeEach(() => {
            sut = new StreamSource();
            sut.resume();
        });

        afterEach(() => {
            sut.destroy();
        });

        describe('when writing null', () => {
            it('should yield an error', (done) => {
                sut.write(null, (err) => {
                    expect(err.message).toEqual('StreamSource->write() requires data to be any of the following types a Buffer, Object, Array, or String');
                    done();
                });
            });
        });

        describe('when writing without a callback', () => {
            it('should throw an error', () => {
                expect(() => sut.write({ hello: true })).toThrowError('StreamSource->write() requires a callback');
            });
        });

        describe('when writing an object', () => {
            it('should return a stream entity', (done) => {
                sut.write({ hello: 'hi' }, (err, result) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    expect(isStreamEntity(result)).toBeTrue();
                    done();
                });
            });
        });

        describe('when writing an string', () => {
            it('should return a stream entity', (done) => {
                sut.write('hello', (err, result) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    expect(isStreamEntity(result)).toBeTrue();
                    done();
                });
            });
        });

        describe('when writing an error', () => {
            it('should cause the stream to emit an error', (done) => {
                sut.write('hello', (err) => {
                    if (err) {
                        done(err);
                    }
                });
                setImmediate(() => {
                    sut.write(new Error('Uh oh'), (err, result) => {
                        if (err) {
                            done(err);
                            return;
                        }
                        expect(result).toBeUndefined();
                        sut.end();
                        done();
                    });
                });
                return sut.toStream().done().then(() => Promise.reject(new Error('Expected stream not to resolve')))
                    .catch((err) => {
                        expect(err.message).toEqual('Uh oh');
                        return Promise.resolve();
                    });
            });
        });

        describe('when the stream is paused', () => {
            it('should return false', (done) => {
                sut.write({ hello: 'hi' }, (err) => {
                    expect(err.message).toEqual('Unable to write to stream');
                    done();
                });
            });
        });
    });
});

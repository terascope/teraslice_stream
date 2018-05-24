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
            sut.toStream().toArray((results) => {
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

        it('should increment with each write', () => {
            _.times(100, (i) => {
                const count = i + 1;
                expect(sut.write({ count })).toBeTruthy();
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

        it('should be ended after writing 100 records', () => {
            sut.resume();
            const after = _.after(100, () => {
                sut.end();
                expect(sut.isEnded()).toBeTrue();
            });
            _.times(100, (i) => {
                expect(sut.write({ i })).toBeTruthy();
                expect(sut.isEnded()).toBeFalse();
                after();
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
            sut.write({ i: 1 });
            expect(sut.isPaused()).toBeTrue();
        });

        it('should be paused after writing 100 records', () => {
            sut.resume();
            const after = _.after(100, () => {
                sut.end();
                expect(sut.isEnded()).toBeTrue();
                expect(sut.isPaused()).toBeTrue();
            });
            _.times(100, (i) => {
                expect(sut.write({ i })).toBeTruthy();
                expect(sut.isPaused()).toBeFalse();
                after();
            });
        });

        it('should be paused after pausing', () => {
            sut.resume();
            const after = _.after(50, _.once(() => {
                sut.pause();
                expect(sut.isPaused()).toBeTrue();
            }));
            _.times(50, (i) => {
                expect(sut.write({ i })).toBeTruthy();
                expect(sut.isPaused()).toBeFalse();
                after();
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

        it('should throw an error if given null', () => {
            expect(() => sut.write(null)).toThrowError('StreamEntity requires data to be any of the following types a Buffer, Object, Array, or String');
        });

        it('should return a stream entity if given an object', () => {
            const result = sut.write({ hello: 'hi' });
            expect(isStreamEntity(result)).toBeTrue();
        });

        it('should return false if the stream is paused', () => {
            sut.pause();
            expect(sut.write({ hello: 'hi' })).toBeFalse();
        });
    });
});

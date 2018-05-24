'use strict';

const {
    StreamEntity, isStreamEntity
} = require('../');

describe('StreamEntity', () => {
    let sut;
    const obj = { example: 'hello' };
    const str = JSON.stringify(obj);
    const buf = Buffer.from(str);

    it('should throw an error if given null', () => {
        expect(() => new StreamEntity(null)).toThrowError();
    });

    describe('when data is a buffer', () => {
        beforeEach(() => {
            sut = new StreamEntity(buf);
        });
        it('should be a stream entity', () => {
            expect(isStreamEntity(sut)).toBeTrue();
        });
        describe('->toBuffer', () => {
            it('it should return same buffer', () => {
                expect(sut.toBuffer()).toEqual(buf);
            });
        });
        describe('->toJSON', () => {
            it('it should return the data in an object', () => {
                expect(sut.toJSON()).toEqual(obj);
            });
        });

        describe('->toString', () => {
            it('it should return the data in string form', () => {
                expect(sut.toString()).toEqual(str);
            });
        });
    });
    describe('when data is a string', () => {
        beforeEach(() => {
            sut = new StreamEntity(str);
        });
        it('should be a stream entity', () => {
            expect(isStreamEntity(sut)).toBeTrue();
        });
        describe('->toBuffer', () => {
            it('it should return same buffer', () => {
                expect(sut.toBuffer()).toEqual(buf);
            });
        });
        describe('->toJSON', () => {
            it('it should return the data in an object', () => {
                expect(sut.toJSON()).toEqual(obj);
            });
        });

        describe('->toString', () => {
            it('it should return the data in string form', () => {
                expect(sut.toString()).toEqual(str);
            });
        });
    });
    describe('when data is a json object', () => {
        beforeEach(() => {
            sut = new StreamEntity(obj);
        });
        it('should be a stream entity', () => {
            expect(isStreamEntity(sut)).toBeTrue();
        });
        describe('->toBuffer', () => {
            it('it should return same buffer', () => {
                expect(sut.toBuffer()).toEqual(buf);
            });
        });
        describe('->toJSON', () => {
            it('it should return the data in an object', () => {
                expect(sut.toJSON()).toEqual(obj);
            });
        });

        describe('->toString', () => {
            it('it should return the data in string form', () => {
                expect(sut.toString()).toEqual(str);
            });
        });
    });
});

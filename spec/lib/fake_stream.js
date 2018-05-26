'use strict';

const async = require('async');
const { promisify } = require('util');

const setTimeoutPromise = promisify(setTimeout);
const { Stream } = require('../../');

module.exports = function fakeReader(batchSize) {
    const stream = new Stream();
    async.times(batchSize, async (i) => {
        await setTimeoutPromise(i * 2);
        await stream.write({ ms: i });
    }, () => {
        stream.end();
    });
    return stream;
};

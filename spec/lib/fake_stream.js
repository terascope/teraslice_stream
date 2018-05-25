'use strict';

const _ = require('lodash');
const { promisify } = require('util');

const setTimeoutPromise = promisify(setTimeout);
const { Stream } = require('../../');

module.exports = function fakeReader(batchSize) {
    const stream = new Stream();
    const records = _.times(batchSize, async (i) => {
        await setTimeoutPromise(i * 2);
        await stream.write({ ms: i });
    });
    Promise.all(records).then(() => {
        stream.end();
    });
    return stream;
};

'use strict';

const _ = require('lodash');
const { StreamSource } = require('../../');

module.exports = function fakeReader(batchSize) {
    const stream = new StreamSource();
    const done = _.after(batchSize, () => {
        stream.end();
    });
    _.times(batchSize, (i) => {
        _.delay(() => {
            stream.write({ ms: i });
            done();
        }, i * 2);
    });
    return stream;
};

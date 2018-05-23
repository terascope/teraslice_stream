'use strict';

const _ = require('lodash');
const H = require('highland');
const { StreamEntity } = require('../../');

module.exports = function fakeReader(batchSize) {
    const stream = H();
    const done = _.after(batchSize, () => {
        stream.end();
    });
    _.times(batchSize, (i) => {
        _.delay(() => {
            stream.write(new StreamEntity({ ms: i }));
            done();
        }, i * 2);
    });
    return stream;
};

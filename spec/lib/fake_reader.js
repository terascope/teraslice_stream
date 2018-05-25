'use strict';

const Promise = require('bluebird');
const _ = require('lodash');
const { StreamSource } = require('../../');

module.exports = function fakeReader(batchSize) {
    const stream = new StreamSource();
    const records = _.times(batchSize);
    Promise.each(records, i => new Promise((resolve, reject) => {
        _.delay(() => {
            stream.write({ ms: i }, (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        }, i * 2);
    })).catch((err) => {
        if (err) {
            stream.write(err, _.noop);
        }
    }).finally(() => {
        stream.end();
    });
    return stream;
};

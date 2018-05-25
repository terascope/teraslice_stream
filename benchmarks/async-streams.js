'use strict';

/* eslint-disable no-console */

const Promise = require('bluebird');
const _ = require('lodash');
const UUID = require('uuid');
const { StreamSource } = require('../');

const streamSource = new StreamSource();
const stream = streamSource.toStream();

const batchSize = 10000;
const records = _.times(batchSize);
Promise.each(records, i => new Promise((resolve, reject) => {
    _.delay(() => {
        stream.write({
            id: UUID.v4(),
            someKey: 'hello',
            index: i,
        }, (err) => {
            if (err) {
                reject(err);
                return;
            }
            if (i === 1) {
                console.log('stream started');
            }
            resolve();
        });
    }, i * 2);
}), (err) => {
    if (err) {
        console.error(err.stack);
        process.exit(1);
        return;
    }
    console.log(`wrote all ${batchSize} records`);
    stream.end();
});

let count = 0;

stream.mapAsync((record, next) => {
    setImmediate(() => {
        record.data.processedAt = Date.now();
        next(null, record);
        if (record.data.index % 101 === 100) {
            console.log('mapAsync', record.data.index);
            stream.pause();
            if (record.data.index > 900) {
                // here
            }
            _.delay(() => {
                stream.resume();
            }, 100);
        }
    });
});

stream.eachAsync((record, next) => {
    let ms = 0;
    if (record.data.index % 101 === 100) {
        ms = 1000;
        console.log('eachAsync', record.data.index);
    }
    _.delay(() => {
        count += 1;
        next(null);
    }, ms);
});

stream.done((err) => {
    if (err) {
        console.error(err.stack);
        process.exit(1);
        return;
    }
    console.log('done', { count });
    process.exit(0);
});

'use strict';

/* eslint-disable no-console */

const async = require('async');
const { promisify } = require('util');
const _ = require('lodash');
const UUID = require('uuid');
const { Stream } = require('../');

const setTimeoutPromise = promisify(setTimeout);

const batchSize = 10000;
const stream = new Stream();
stream.on('error', (err) => {
    console.error('GOT ERROR', err);
});
async.times(batchSize, async (i) => {
    await setTimeoutPromise(i * 2);
    await stream.write({
        id: UUID.v4(),
        someKey: 'hello',
        index: i,
    });
    if (i === 1) {
        console.log('stream started');
    }
}, (err) => {
    if (err) {
        console.error(err.stack);
        process.exit(1);
        return;
    }
    console.log(`wrote all ${batchSize} records`);
    stream.end();
});

let count = 0;

stream.map(async (record) => {
    setTimeoutPromise(10);
    record.data.processedAt = Date.now();
    if (record.data.index % 101 === 100) {
        console.log('map', record.data.index);
        stream.pause();
        _.delay(() => {
            stream.resume();
        }, 100);
    }
    return record;
});

stream.each(async (record) => {
    let ms = 0;
    if (record.data.index % 101 === 100) {
        ms = 10;
        console.log('each', record.data.index);
    }
    await setTimeoutPromise(ms);
    count += 1;
});

stream.done().then(() => {
    console.log('done', { count });
    process.exit(0);
}).catch((err) => {
    console.error(err.stack);
    process.exit(1);
});

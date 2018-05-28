'use strict';

/* eslint-disable no-console */

const async = require('async');
const { promisify } = require('util');
const _ = require('lodash');
const uuidv1 = require('uuid/v1');
const { Stream } = require('../');

const setTimeoutPromise = promisify(setTimeout);

const batchSize = 30000;
const stream = new Stream();
stream.once('ready', () => {
    console.log('AsyncStreamsBenchmark started');
    console.time('AsyncStreamsBenchmark');
});
stream.once('finished', () => {
    console.timeEnd('AsyncStreamsBenchmark');
});
stream.on('error', (err) => {
    console.error('GOT ERROR', err);
});
async.times(batchSize, async (i) => {
    await setTimeoutPromise(i);
    await stream.write({
        id: uuidv1(),
        someKey: 'hello',
        index: i,
    });
}, (err) => {
    if (err) {
        console.error(err.stack);
        process.exit(1);
        return;
    }
    stream.end();
});

let count = 0;

stream.map(async (record) => {
    setTimeoutPromise(10);
    record.data.processedAt = Date.now();
    if (record.data.index % 1001 === 1000) {
        stream.pause();
        _.delay(() => {
            stream.resume();
        }, 10);
    }
    return record;
});

stream.each(async (record) => {
    let ms = 0;
    if (record.data.index % 1001 === 1000) {
        ms = 10;
    }
    await setTimeoutPromise(ms);
    count += 1;
});

stream.done().then(() => {
    console.log('AsyncStreamsBenchmark done!', { count });
    process.exit(0);
}).catch((err) => {
    console.error(err.stack);
    process.exit(1);
});

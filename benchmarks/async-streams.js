'use strict';

/* eslint-disable no-console */

const _ = require('lodash');
const Promise = require('bluebird');
const { Stream } = require('../');

const batchSize = 10000;
const stream = new Stream();
stream.on('error', (err) => {
    console.error('GOT ERROR', err);
});
console.log(`AsyncStreamsBenchmark started with a batchSize of ${batchSize}`);
console.time('AsyncStreamsBenchmark');
function write(i) {
    return Promise.map(_.times(10), n => stream
        .write({
            id: 'some-random-key',
            someKey: 'hello',
            anotherKey: 'hi',
            index: i + n,
            r: Math.random(),
            k: Math.random(),
            m: Math.random(),
        }, { key: 'some-random-key' })).delay(10);
}
Promise.each(_.times(batchSize / 10), write).then(() => stream.end());
let count = 0;

stream.map((record) => {
    record.data.processedAt = Date.now();
    return Promise.delay(0).then(() => record);
});

stream.each(() => {
    count += 1;
});

stream.done().then(() => {
    console.timeEnd('AsyncStreamsBenchmark');
    console.log(`AsyncStreamsBenchmark finished with a count of ${count}`);
    process.exit(0);
}).catch((err) => {
    console.error(err.stack);
    process.exit(1);
});

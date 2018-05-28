'use strict';

const Promise = require('bluebird');

function pWaterfall(iterable, initVal) {
    return Promise.reduce(iterable, (prev, fn) => fn(prev), initVal);
}

module.exports = { pWaterfall };

'use strict';

const Promise = require('bluebird');

const pWaterfall = (iterable, initVal) => Promise.reduce(iterable, (prev, fn) => fn(prev), initVal);

module.exports = { pWaterfall };

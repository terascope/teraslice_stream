'use strict';

const _ = require('lodash');
const Promise = require('bluebird');
const pBreak = require('p-break');
const pEvent = require('p-event');
const { EventEmitter } = require('events');
const { asyncify, queue } = require('async');
const { pWaterfall } = require('./utils');
const StreamEntity = require('./stream_entity');

class Stream extends EventEmitter {
    constructor({ concurrency = 1 } = {}) {
        super();
        this._handleMessage = this._handleMessage.bind(this);
        this._queue = queue(asyncify(this._handleMessage), concurrency);
        this._queue.pause();
        this._queue.error = (err) => {
            this._isFailed = true;
            this._queue._tasks.empty();
            this.emit('error', err);
        };
        this._queue.drain = () => {
            if (this._isFull) {
                this._isEnded = true;
                this.emit('finished');
            }
            this.emit('queue:drain');
        };
        this.on('ready', () => {
            this._isReady = true;
            this._queue.resume();
        });
        const _ready = () => {
            if (this._isReady) {
                return;
            }
            if (!this._isConsumed) {
                this._isReady = false;
                return;
            }
            this.emit('ready');
        };
        this.on('add:pipeline', () => {
            _ready();
        });
        this.on('add:consumer', () => {
            this._isConsumed = true;
            _ready();
        });
        this._isConsumed = false;
        this._isEnded = false;
        this._isFailed = false;
        this._isFull = false;
        this._pipeline = [];
        this._processed = 0;
        this._records = [];
        this._shouldStoreRecords = false;
    }

    destroy() {
        this._isReady = false;
        this._isFull = false;
        this._isConsumed = false;
        this._shouldStoreRecords = false;
        this._processed = 0;
        this._records.length = 0;
        this._pipeline.length = 0;
        this._queue.kill();
        return this;
    }

    done() {
        this.emit('add:consumer');
        return Promise.resolve(pEvent(this, 'finished'));
    }

    each(fn) {
        this._pipeline.push((record) => {
            const p = Promise.resolve(fn(record));
            return p.then(() => record);
        });
        this.emit('add:pipeline');
        return this;
    }

    end() {
        this._isFull = true;
        this.emit('queue:full');
        return this;
    }

    filter(fn) {
        this._pipeline.push((record) => {
            const p = Promise.resolve(fn(record));
            return p.then(r => (r ? record : pBreak()));
        });
        this.emit('add:pipeline');
        return this;
    }

    map(fn) {
        this._pipeline.push(fn);
        this.emit('add:pipeline');
        return this;
    }

    isPaused() {
        return this._queue.paused;
    }

    isEnded() {
        return this._isEnded;
    }

    pause() {
        this._queue.pause();
        return this;
    }

    resume() {
        this._queue.resume();
        return this;
    }

    stats() {
        return {
            processed: this._processed,
            running: this._queue.running(),
            pending: this._queue.length(),
        };
    }

    toArray() {
        this._shouldStoreRecords = true;
        if (_.isEmpty(this._pipeline)) {
            this.each(r => r);
            this.resume();
        }
        return this.done().then(() => this._records);
    }

    async write(data, options) {
        if (this._isFull) {
            return Promise.reject(new Error('Cannot write to stream because it is marked as full'));
        }
        if (this._isEnded) {
            return Promise.reject(new Error('Cannot write to a stream because it is marked as ended'));
        }
        if (_.isError(data)) {
            this._queue.push(data);
            return null;
        }
        const records = [];
        if (_.isArray(data)) {
            const validRecords = _.every(data, (item) => {
                if (item instanceof StreamEntity) {
                    records.push(item);
                    return true;
                }
                return false;
            });
            if (!validRecords) {
                return Promise.reject(new Error('Stream->write() requires an array of StreamEntities or data'));
            }
        } else if (data instanceof StreamEntity) {
            records.push(data);
        } else {
            records.push(new StreamEntity(data, options));
        }
        if (_.size(records) === 0) {
            return null;
        }
        this._queue.push(records);
        if (_.size(records) === 1) {
            return _.first(records);
        }
        return records;
    }

    _handleMessage(message) {
        if (_.isError(message)) {
            return Promise.reject(message);
        }
        return pWaterfall(this._pipeline, message).catch(pBreak.end).then((result) => {
            this._processed += 1;
            if (this._shouldStoreRecords && result instanceof StreamEntity) {
                this._records.push(result);
            }
        });
    }
}

module.exports = Stream;

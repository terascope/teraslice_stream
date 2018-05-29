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
        const _finish = () => {
            if (!this._shouldEnd) {
                return;
            }
            if (!this._queue.idle()) {
                return;
            }
            this._isEnded = true;
            this.emit('finished');
        };
        this.on('end', () => {
            _finish();
        });
        this._queue.drain = () => {
            _finish();
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
        this._shouldEnd = false;
        this._pipeline = [];
        this._processed = 0;
        this._records = [];
        this._shouldStoreRecords = false;
    }

    destroy() {
        this._isReady = false;
        this._shouldEnd = false;
        this._isConsumed = false;
        this._shouldStoreRecords = false;
        this._processed = 0;
        this._records.length = 0;
        this._pipeline.length = 0;
        this.removeAllListeners();
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
        this._shouldEnd = true;
        this.emit('end');
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
            this.resume();
        }
        return this.done().then(() => this._records);
    }

    write(input, options) {
        let data = input;
        if (this._shouldEnd) {
            return Promise.reject(new Error('Cannot write to stream because it is marked as full'));
        }
        if (this._isEnded) {
            return Promise.reject(new Error('Cannot write to a stream because it is marked as ended'));
        }
        if (_.isError(data)) {
            this._queue.push(data);
            return Promise.resolve();
        }
        if (!data) {
            return Promise.reject(new Error('Cannot write empty data to stream'));
        }
        if (_.isEmpty(data)) {
            return Promise.resolve();
        }
        if (StreamEntity.validateData(data)) {
            data = new StreamEntity(data, options);
        }
        data = _.castArray(data);
        const validInput = _.every(data, record => record instanceof StreamEntity);
        if (!validInput) {
            return Promise.reject(new Error('Invalid input to Stream->write()'));
        }
        const records = _.map(data, (record) => {
            if (record instanceof StreamEntity) {
                return record;
            }
            return new StreamEntity(record);
        });
        this._queue.push(records);
        if (_.size(records) === 1) {
            return Promise.resolve(_.first(records));
        }
        return Promise.resolve(records);
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

'use strict';

const _ = require('lodash');
const { EventEmitter } = require('events');
const { asyncify, queue, waterfall } = require('async');
const StreamEntity = require('./stream_entity');

const waterfallAsync = pipeline => new Promise((resolve, reject) => {
    waterfall(pipeline, (err, result) => {
        if (err) {
            reject(err);
            return;
        }
        resolve(result);
    });
});

const validateInput = (input) => {
    if (_.isEmpty(input)) {
        return;
    }
    if (!_.isArray(input)) {
        throw new Error('StreamSource requires an array for the input');
    }
    const containsStreamEntities = _.every(input, r => r instanceof StreamEntity);
    if (!containsStreamEntities) {
        throw new Error('StreamSource requires an array of StreamEntities as input');
    }
};

class Stream extends EventEmitter {
    constructor(input) {
        super();
        this._readyToPush = this._readyToPush.bind(this);
        this._handleMessage = this._handleMessage.bind(this);
        this._queue = queue(asyncify(this._handleMessage), 1);
        if (input) {
            validateInput(input);
        }
        this._queue.error = (error) => {
            this._end = true;
            this._queue.kill();
            this.emit('error', error);
        };
        this._queue.drain = () => {
            if (this._end) {
                this.ended = true;
                setTimeout(() => {
                    this.emit('finished');
                }, 50);
            }
            this.emit('drain');
        };
        this.once('ready', () => {
            if (_.isEmpty(input)) return;
            this._queue.push(input);
        });
        const _ready = () => {
            if (!this._consumed) {
                this.ready = false;
                return;
            }
            if (this.ready) {
                return;
            }
            this.emit('ready');
            this.ready = true;
        };
        this.on('end', () => {
            this._end = true;
        });
        this.on('add:pipeline', (fn) => {
            this._pipeline.push(asyncify(fn));
            _ready();
        });
        this.on('add:consumer', () => {
            this._consumed = true;
            _ready();
        });
        this._storeRecords = false;
        this._consumed = false;
        this._pipeline = [];
        this._end = false;
        this.ended = true;
        this._records = [];
        this._processed = 0;
    }

    done() {
        this.emit('add:consumer');
        return new Promise((resolve, reject) => {
            this.once('error', (err) => {
                reject(err);
            });
            this.once('finished', () => {
                resolve();
            });
        });
    }

    end() {
        this.emit('end');
        return this;
    }

    filter(fn) {
        this.emit('add:pipeline', async (record) => {
            if (await fn(record)) {
                return record;
            }
            return record;
        });
        return this;
    }

    map(fn) {
        this.emit('add:pipeline', async record => fn(record));
        return this;
    }

    each(fn) {
        this.emit('add:pipeline', async (record) => {
            await fn(record);
        });
        return this;
    }

    isPaused() {
        return this._queue.paused;
    }

    pause() {
        this._queue.pause();
        return this;
    }

    resume() {
        this._queue.resume();
        return this;
    }

    async toArray() {
        this._storeRecords = true;
        await this.done();
        return this._records;
    }

    status() {
        return {
            processed: this._processed,
            finished: _.size(this._records),
            running: this._queue.running(),
            pending: this._queue.length(),
        };
    }

    destroy() {
        this.ready = false;
        this._processed = 0;
        this._records.length = 0;
        this._pipeline.length = 0;
        this._readyHandlers = [];
        this._queue.kill();
        return this;
    }

    async write(data, options) {
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
            return Promise.reject(new Error('No records to write'));
        }
        await this._readyToPush();
        this._queue.push(records);
        if (_.size(records) === 1) {
            return _.first(records);
        }
        return records;
    }

    async _handleMessage(message) {
        if (_.isError(message)) {
            return Promise.reject(message);
        }
        const pipeline = this._pipeline;
        pipeline.unshift(asyncify(async () => message));
        const record = await waterfallAsync(pipeline);
        if (this._storeRecords) {
            if (record) {
                this._records.push(record);
            }
        }
        this._processed += 1;
        return Promise.resolve();
    }

    _readyToPush() {
        if (this.ready) {
            return Promise.resolve();
        }
        this._setupReadyHandlers();
        return new Promise((resolve, reject) => {
            this._readyHandlers.push((err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
    }

    _setupReadyHandlers() {
        if (!_.isEmpty(this._readyHandlers)) {
            return;
        }
        this._readyHandlers = [];
        const callback = _.once((err) => {
            this.removeListener('error', errorHandler);
            this.removeListener('ended', endHandler);
            this.removeListener('ready', readyHandler);
            _.each(this._readyHandlers, (handler) => {
                handler(err);
            });
        });
        function errorHandler(err) {
            callback(err);
        }
        function endHandler() {
            callback(new Error('Stream Ended'));
        }
        function readyHandler() {
            callback(null);
        }
        this.once('error', errorHandler);
        this.once('end', endHandler);
        this.once('ready', readyHandler);
    }
}

module.exports = Stream;

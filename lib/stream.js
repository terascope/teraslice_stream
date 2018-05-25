'use strict';

const Promise = require('bluebird');
const H = require('highland');
const _ = require('lodash');

class Stream {
    constructor(source) {
        if (_.isEmpty(source) && !source.__isTerasliceStreamSource) {
            throw new Error('Stream requires a StreamSource');
        }
        this._source = source;
        this._stream = source._stream;
    }

    done(_cb) {
        if (!_.isFunction(_cb)) {
            return new Promise((resolve, reject) => {
                this._stream.stopOnError((err) => {
                    reject(err);
                }).done(() => {
                    resolve(null);
                });
            });
        }
        const cb = _.once(_cb);
        this._stream.stopOnError((err) => {
            cb(err);
        }).done(() => {
            cb(null);
        });
        return this;
    }

    each(fn) {
        this._stream = this._stream.each(fn);
        return this;
    }

    eachAsync(fn) {
        this._stream = this._stream.consume((err, record, push, next) => {
            if (err) {
                push(err);
                next();
                return;
            }
            if (record === H.nil) {
                push(null, record);
                return;
            }
            this._onStreamReady((err) => {
                if (err) {
                    push(null, H.nil);
                    return;
                }
                fn(record, (err) => {
                    if (err) {
                        push(err);
                    }
                    next();
                });
            });
        });
        return this;
    }

    end() {
        this._source.end();
        return this;
    }

    filter(fn) {
        this._stream = this._stream.filter(fn);
        return this;
    }

    isPaused() {
        return this._stream.paused;
    }

    isEnded() {
        return this._stream.ended;
    }

    map(fn) {
        this._stream = this._stream.map(fn);
        return this;
    }

    mapAsync(fn) {
        this._stream = this._stream.consume((err, record, push, next) => {
            if (err) {
                push(err);
                next();
                return;
            }
            if (record === H.nil) {
                push(null, record);
                return;
            }
            this._onStreamReady((err) => {
                if (err) {
                    push(null, H.nil);
                    return;
                }
                fn(record, (err, newRecord) => {
                    push(err, newRecord);
                    next();
                });
            });
        });
        return this;
    }


    pause() {
        this._source.pause();
        return this;
    }

    resume() {
        this._source.resume();
        return this;
    }

    toArray(_cb) {
        if (!_.isFunction(_cb)) {
            const stream = this._stream.collect();
            if (!this._stream) {
                throw new Error('Stream is gone');
            }
            return stream.toPromise(Promise);
        }
        const cb = _.once(_cb);
        this._stream.stopOnError((err) => {
            cb(err);
        }).toArray((results) => {
            cb(null, results);
        });
        if (!this._stream) {
            throw new Error('Stream is gone');
        }
        return this;
    }

    toStream(_cb) {
        if (!_.isFunction(_cb)) {
            return this.toArray().then(records => this._replaceStream(records));
        }
        const cb = _.once(_cb);
        this.toArray((err, records) => {
            if (err) {
                cb(err);
                return;
            }
            cb(null, this._replaceStream(records));
        });
        return this;
    }

    _onStreamReady(callback) {
        if (this.isEnded()) {
            callback(new Error('Stream ended'));
            return;
        }
        if (!this.isPaused()) {
            callback();
            return;
        }
        setImmediate(() => {
            this._onStreamReady(callback);
        });
    }

    _replaceStream(input) {
        this._source._replaceStream(input);
        this._stream = this._source._stream;
        if (!this._stream) {
            throw new Error('Stream is gone');
        }
        return this;
    }
}

module.exports = Stream;

'use strict';

const autoBind = require('auto-bind');
const H = require('highland');
const _ = require('lodash');

class Stream {
    constructor(input) {
        if (!_.isArray(input) && !H.isStream(input)) {
            throw new Error('Stream expects a highland stream source or an array');
        }
        this._stream = H.isStream(input) ? input : H(input);
        autoBind(this);
    }

    done(_cb) {
        const cb = _.once(_cb);
        this._stream.stopOnError((err) => {
            cb(err);
        }).done(() => {
            cb(null);
        });
    }

    each(fn) {
        this._stream = this._stream.each(fn);
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
    }

    isPaused() {
        return this._stream.paused;
    }

    map(fn) {
        this._stream = this._stream.map(fn);
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
    }


    pause() {
        this._stream.pause();
    }

    resume() {
        this._stream.resume();
    }

    toArray(_cb) {
        const cb = _.once(_cb);
        this._stream.stopOnError((err) => {
            cb(err);
        }).toArray((results) => {
            cb(null, results);
        });
    }

    _onStreamReady(callback) {
        if (this.ended) {
            callback(new Error('Stream ended'));
            return;
        }
        if (!this.paused) {
            callback();
            return;
        }
        setImmediate(() => {
            this._onStreamReady(callback);
        });
    }
}

module.exports = Stream;

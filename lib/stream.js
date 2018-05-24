'use strict';

const Promise = require('bluebird');
const H = require('highland');
const _ = require('lodash');
const StreamSource = require('./stream_source');

class Stream {
    constructor(source) {
        if (source instanceof StreamSource) {
            this._stream = source.toStream();
        } else {
            this._stream = new StreamSource(source).toStream();
        }
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

    filter(fn) {
        this._stream = this._stream.filter(fn);
        return this;
    }

    isPaused() {
        return this._stream.paused;
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
        this._stream.pause();
        return this;
    }

    resume() {
        this._stream.resume();
        return this;
    }

    toArray(_cb) {
        if (!_.isFunction(_cb)) {
            return this._stream.collect().toPromise(Promise);
        }
        const cb = _.once(_cb);
        this._stream.stopOnError((err) => {
            cb(err);
        }).toArray((results) => {
            cb(null, results);
        });
        return this;
    }

    toStream(_cb) {
        if (!_.isFunction(_cb)) {
            return this.toArray().then(records => new Stream(records));
        }
        const cb = _.once(_cb);
        this.toArray((err, records) => {
            if (err) {
                cb(err);
                return;
            }
            cb(null, new Stream(records));
        });
        return this;
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

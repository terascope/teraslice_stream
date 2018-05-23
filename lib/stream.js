'use strict';

const autoBind = require('auto-bind');
const H = require('highland');
const _ = require('lodash');

class Stream {
    constructor(reader) {
        const stream = H();
        this._reader = reader;
        this._stream = stream;
        this._reader.takeNext(stream);
        autoBind(this);
    }

    done(_cb) {
        const cb = _.once(_cb);
        this._stream = this._stream.stopOnError((err) => {
            cb(err);
        });
        this._stream = this._stream.done(() => {
            cb();
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
            this._streamReady(() => {
                fn(record, (fnErr) => {
                    if (fnErr) {
                        push(fnErr);
                    }
                    next();
                });
            });
        });
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
            this._streamReady(() => {
                fn(record, (fnErr, newRecord) => {
                    push(fnErr, newRecord);
                    next();
                });
            });
        });
    }

    _streamReady(cb) {
        cb();
    }
}

module.exports = Stream;


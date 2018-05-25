'use strict';

const H = require('highland');
const _ = require('lodash');
const StreamEntity = require('./stream_entity');

const { validateData } = StreamEntity;
const Stream = require('./stream');

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

function StreamError(err) {
    this.__HighlandStreamError__ = true;
    this.error = err;
}

class StreamSource {
    constructor(input) {
        validateInput(input);
        this.__isTerasliceStreamSource = true;
        this._stream = H(input);
        this.count = 0;
    }

    end() {
        return this._stream.end();
    }

    destroy() {
        this.count = 0;
        return this._stream.destroy();
    }

    isEnded() {
        return this._stream.ended;
    }

    isPaused() {
        return this._stream.paused;
    }

    pause() {
        this._stream.pause();
    }

    resume() {
        this._stream.resume();
    }

    toStream() {
        return new Stream(this);
    }

    write(data, ...rest) {
        let callback;
        let options;
        if (_.isFunction(rest[0])) {
            [callback] = rest;
        } else if (_.isFunction(rest[1])) {
            [options, callback] = rest;
        }
        if (!_.isFunction(callback)) {
            throw new Error('StreamSource->write() requires a callback');
        }
        if (!_.isError(data) && !validateData(data)) {
            callback(new Error('StreamSource->write() requires data to be any of the following types a Buffer, Object, Array, or String'));
            return;
        }
        const entity = !_.isError(data) ? new StreamEntity(data, options) : undefined;
        let attempts = 0;
        const sendMessageWhenReady = () => {
            if (this.isEnded()) {
                callback(new Error('Stream ended'));
                return;
            }
            if (!this.isPaused()) {
                const message = _.isError(data) ? new StreamError(data) : entity;
                this._stream.write(message);
                this.count += 1;
                callback(null, entity);
                return;
            }
            attempts += 1;
            if (attempts > 50) {
                callback(new Error('Unable to write to stream'));
                return;
            }
            setImmediate(() => {
                sendMessageWhenReady();
            });
        };
        sendMessageWhenReady();
    }

    _replaceStream(input) {
        this._stream.destroy();
        validateInput(input);
        this._stream = H(input);
        this.count = 0;
        return this;
    }
}

module.exports = StreamSource;

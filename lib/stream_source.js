'use strict';

const H = require('highland');
const _ = require('lodash');
const StreamEntity = require('./stream_entity');

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

class StreamSource {
    constructor(input) {
        validateInput(input);
        this._stream = H(input);
        this.count = 0;
    }

    end() {
        return this._stream.end();
    }

    destroy() {
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
        return this._stream;
    }

    write(data, options) {
        const entity = new StreamEntity(data, options);
        const result = this._stream.write(entity);
        if (result === false) {
            return false;
        }
        this.count += 1;
        return entity;
    }
}

module.exports = StreamSource;

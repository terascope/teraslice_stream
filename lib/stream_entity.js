'use strict';

const autoBind = require('auto-bind');
const uuid = require('uuid');
const _ = require('lodash');

/**
 * data - The data item, type of this field is open but most likely an object.
 * options - A object containing the following properties
 *  - key - A unique key for the data entity
 *  - ingestTime - The time the data was ingested if available. This must be
 *      provided by the source of the data.
 *  - processTime - Time set by the reader at the point when the data is initially
 *      brought into the Teraslice pipeline.
 *  - eventTime - Time associated with actual event that is extracted from the
 *      data record.
 */

class StreamEntity {
    constructor(data, opts) {
        const options = opts || {};
        this.data = data;
        this.key = options.key || uuid.v4();
        this.ingestTime = options.ingestTime;
        this.processTime = options.processTime || new Date();
        this.eventTime = options.eventTime;
        autoBind(this);
    }

    toString() {
        if (_.isString(this.data)) {
            return this.data;
        }
        if (_.isBuffer(this.data)) {
            return _.toString(this.data);
        }
        return JSON.stringify(this.data);
    }

    toJSON() {
        const data = _.isBuffer(this.data) ? _.toString(this.data) : this.data;
        if (!_.isString(data)) {
            throw new Error('StreamEntity is not JSON parsable');
        }
        return JSON.parse(this.data);
    }

    toBuffer() {
        if (_.isBuffer(this.data)) {
            return this.data;
        }
        return Buffer.from(this.toString());
    }
}

module.exports = StreamEntity;


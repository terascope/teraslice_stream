'use strict';

const Stream = require('./lib/stream');
const StreamEntity = require('./lib/stream_entity');

module.exports = Stream;
module.exports.StreamEntity = StreamEntity;
module.exports.Stream = Stream;
module.exports.isStream = input => input instanceof Stream;
module.exports.isStreamEntity = input => input instanceof StreamEntity;

'use strict';

const Stream = require('./lib/stream');
const StreamEntity = require('./lib/stream_entity');
const StreamSource = require('./lib/stream_source');

module.exports = Stream;
module.exports.Stream = Stream;
module.exports.StreamEntity = StreamEntity;
module.exports.StreamSource = StreamSource;
module.exports.isStream = input => input instanceof Stream;
module.exports.isStreamEntity = input => input instanceof StreamEntity;
module.exports.isStreamSource = input => input instanceof StreamSource;

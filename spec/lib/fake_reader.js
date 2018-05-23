'use strict';

const _ = require('lodash');
const StreamEntity = require('../../').StreamEntity;

class FakeReader {
    constructor(size) {
        this.size = size || 100;
    }
    takeNext(stream) {
        const done = _.after(this.size, () => {
            stream.end();
        });
        _.times(this.size, (i) => {
            _.delay(() => {
                stream.write(new StreamEntity({ ms: i }));
                done();
            }, i * 2);
        });
    }
}

module.exports = FakeReader;


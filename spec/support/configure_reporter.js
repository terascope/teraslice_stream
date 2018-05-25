'use strict';

const { SpecReporter } = require('jasmine-spec-reporter');

jasmine.getEnv().addReporter(new SpecReporter({
    spec: {
        displayStacktrace: true
    },
    summary: {
        displayStacktrace: false
    }
}));

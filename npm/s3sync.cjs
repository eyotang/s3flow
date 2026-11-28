#!/usr/bin/env node
'use strict';
require('./run.cjs').run('s3sync', process.argv.slice(2));

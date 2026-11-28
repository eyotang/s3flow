#!/usr/bin/env node
'use strict';
require('./run.cjs').run('s3mv', process.argv.slice(2));

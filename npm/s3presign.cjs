#!/usr/bin/env node
'use strict';
require('./run.cjs').run('s3presign', process.argv.slice(2));

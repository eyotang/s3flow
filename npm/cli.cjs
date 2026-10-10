#!/usr/bin/env node
'use strict';

const { run } = require('./run.cjs');
const [command, ...args] = process.argv.slice(2);
if (!command || command === '--help' || command === '-h') {
  console.log('Usage: s3flow <s3get|s3push> [arguments]\n\n  s3get   Download S3 files or directories\n  s3push  Upload files or directories\n\nRun s3flow <command> --help for command options.');
} else {
  run(command, args);
}

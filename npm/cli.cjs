#!/usr/bin/env node
'use strict';

const { run } = require('./run.cjs');
const [command, ...args] = process.argv.slice(2);
if (!command || command === '--help' || command === '-h') {
  console.log('Usage: s3flow <install|s3get|s3push> [arguments]\n\n  install Install s3get and s3push globally\n  s3get   Download S3 files or directories\n  s3push  Upload files or directories\n\nRun s3flow --version for the package version.\nRun s3flow <command> --help for command options.');
} else if (command === '--version' || command === '-version') {
  console.log(require('../package.json').version);
} else if (command === 'install') {
  require('./install.cjs').install(args);
} else {
  run(command, args);
}

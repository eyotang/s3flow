#!/usr/bin/env node
'use strict';

const args = process.argv.slice(2);
if (args[0] === 'install') {
  require('./install.cjs').install(args.slice(1));
} else {
  require('./run.cjs').run('s3flow', args);
}

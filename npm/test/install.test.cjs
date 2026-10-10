'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { parseArgs } = require('../install.cjs');

test('installer accepts default prefix, help and paths containing spaces', () => {
  assert.deepEqual(parseArgs([]), {});
  assert.deepEqual(parseArgs(['--help']), { help: true });
  assert.deepEqual(parseArgs(['--prefix', 'my tools']), { prefix: path.resolve('my tools') });
});

test('installer rejects missing prefix values and unexpected arguments', () => {
  for (const args of [['--prefix'], ['--prefix', ''], ['--prefix', '--force'], ['--force'], ['other']]) {
    assert.throws(() => parseArgs(args), /Usage/);
  }
});

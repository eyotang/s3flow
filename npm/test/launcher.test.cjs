'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const path = require('node:path');
const { binaryPath } = require('../run.cjs');

test('resolves the unified binary for all six platforms', () => {
  for (const platform of ['win32', 'darwin', 'linux']) {
    for (const arch of ['x64', 'arm64']) {
      for (const command of ['s3flow']) {
        assert.equal(binaryPath(command, platform, arch), path.resolve(__dirname, '..', 'native', `${platform}-${arch}`, command + (platform === 'win32' ? '.exe' : '')));
      }
    }
  }
});

test('rejects unknown commands and unsupported platforms', () => {
  assert.throws(() => binaryPath('../other'), /Unknown command/);
  assert.throws(() => binaryPath('s3flow', 'freebsd', 'x64'), /Unsupported platform/);
  assert.throws(() => binaryPath('s3flow', 'linux', 'ia32'), /Unsupported platform/);
});

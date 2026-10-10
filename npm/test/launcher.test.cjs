'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const { binaryPath } = require('../run.cjs');

test('selects the correct binary for all six platforms and both commands', () => {
  for (const platform of ['win32', 'darwin', 'linux']) {
    for (const arch of ['x64', 'arm64']) {
      for (const command of ['s3get', 's3push']) {
        assert.equal(binaryPath(command, platform, arch), path.resolve(__dirname, '..', 'native', `${platform}-${arch}`, command + (platform === 'win32' ? '.exe' : '')));
      }
    }
  }
});

test('rejects unknown commands and unsupported platforms', () => {
  assert.throws(() => binaryPath('../other'), /Unknown command/);
  assert.throws(() => binaryPath('s3get', 'freebsd', 'x64'), /Unsupported platform/);
  assert.throws(() => binaryPath('s3push', 'linux', 'ia32'), /Unsupported platform/);
});

test('dispatcher provides help and fails on unknown commands', () => {
  const cli = path.resolve(__dirname, '../cli.cjs');
  const help = spawnSync(process.execPath, [cli, '--help'], { encoding: 'utf8' });
  assert.equal(help.status, 0);
  assert.match(help.stdout, /s3get.*s3push/);
  const invalid = spawnSync(process.execPath, [cli, 'other'], { encoding: 'utf8' });
  assert.equal(invalid.status, 1);
  assert.match(invalid.stderr, /Unknown command/);
});

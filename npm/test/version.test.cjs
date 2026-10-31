const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

test('version flags print the npm package version', () => {
  for (const flag of ['--version', '-version']) {
    const result = spawnSync(process.execPath, [path.join(__dirname, '../cli.cjs'), flag], { encoding: 'utf8' });
    assert.equal(result.status, 0);
    assert.equal(result.stdout, require('../../package.json').version + '\n');
    assert.equal(result.stderr, '');
  }
});

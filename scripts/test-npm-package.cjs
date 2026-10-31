'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 's3flow-npx-'));
const npmCLI = process.env.npm_execpath;
if (!npmCLI) throw new Error('Run this check with npm run test:package');

function execute(binary, args, options = {}) {
  const result = spawnSync(binary, args, {
    cwd: root,
    encoding: 'utf8',
    env: { ...process.env, npm_config_cache: path.join(temp, 'cache') },
    ...options,
  });
  assert.ifError(result.error);
  return result;
}

try {
  let tarball = process.argv[2] && path.resolve(process.argv[2]);
  if (!tarball) {
    const stage = path.join(temp, 'package');
    fs.mkdirSync(path.join(stage, 'npm'), { recursive: true });
    for (const file of ['package.json', 'README.md', 'LICENSE']) {
      fs.copyFileSync(path.join(root, file), path.join(stage, file));
    }
    for (const file of fs.readdirSync(path.join(root, 'npm')).filter(name => name.endsWith('.cjs'))) {
      fs.copyFileSync(path.join(root, 'npm', file), path.join(stage, 'npm', file));
    }
    const native = path.join(stage, 'npm', 'native', `${process.platform}-${process.arch}`);
    fs.mkdirSync(native, { recursive: true });
    for (const command of ['s3flow']) {
      const binary = path.join(native, command + (process.platform === 'win32' ? '.exe' : ''));
      const result = execute('go', ['build', '-o', binary, `./cmd/${command}`]);
      assert.equal(result.status, 0, result.stderr);
    }
    const packed = execute(process.execPath, [npmCLI, 'pack', '--ignore-scripts', '--json', '--pack-destination', temp], { cwd: stage });
    assert.equal(packed.status, 0, packed.stderr);
    const archives = fs.readdirSync(temp).filter(name => name.endsWith('.tgz'));
    assert.equal(archives.length, 1, packed.stdout);
    tarball = path.join(temp, archives[0]);
  }

  // Install into an isolated cache, outside the repository, with scripts and
  // network disabled. This exercises the real npm bin shims, including Windows.
  const env = { ...process.env, npm_config_cache: path.join(temp, 'cache') };
  function npmExec(args) {
    return execute(process.execPath, [npmCLI, 'exec', '--yes', '--offline', '--ignore-scripts', `--package=${tarball}`, '--', ...args], { cwd: temp, env });
  }
  for (const command of ['s3get', 's3push']) {
    const inferred = execute(process.execPath, [npmCLI, 'exec', '--yes', '--offline', '--ignore-scripts', '--', `file:${tarball}`, command, '--help'], { cwd: temp, env });
    assert.equal(inferred.status, 0, inferred.stderr);
    assert.match(inferred.stdout + inferred.stderr, /-concurrency N/);
    const subcommand = command === 's3get' ? 'get' : 'push';
    for (const args of [[command, '--help'], ['s3flow', command, '--help'], ['s3flow', subcommand, '--help']]) {
      const result = npmExec(args);
      assert.equal(result.status, 0, result.stderr);
      assert.match(result.stdout + result.stderr, /-concurrency N/);
      assert.ok(result.stdout.includes(`s3flow ${subcommand} [flags]`), 'Dispatched to the wrong native command');
    }
    const version = npmExec(['s3flow', subcommand, '--version']);
    assert.equal(version.status, 0, version.stderr);
    assert.ok(version.stdout.startsWith(`s3flow ${subcommand} `), version.stdout);
    const invalid = npmExec([command, '--not-a-real-option']);
    assert.notEqual(invalid.status, 0, 'Native failures must propagate through npx');
    assert.match(invalid.stdout + invalid.stderr, /not-a-real-option/);
    const invalidSubcommand = npmExec(['s3flow', subcommand, '--not-a-real-option']);
    assert.equal(invalidSubcommand.status, invalid.status);
    assert.match(invalidSubcommand.stderr, /not-a-real-option/);
  }
  const rootHelp = npmExec(['s3flow', '--help']);
  assert.equal(rootHelp.status, 0, rootHelp.stderr);
  assert.match(rootHelp.stdout, /get/);
  assert.match(rootHelp.stdout, /push/);
  const rootVersion = npmExec(['s3flow', '--version']);
  assert.equal(rootVersion.status, 0, rootVersion.stderr);
  assert.match(rootVersion.stdout, /^s3flow /);
  const unknown = npmExec(['s3flow', 'unknown']);
  assert.equal(unknown.status, 2);
  const prefix = path.join(temp, 'global install');
  const installed = npmExec(['s3flow', 'install', '--prefix', prefix]);
  assert.equal(installed.status, 0, installed.stdout + installed.stderr);
  // The installed package must survive deletion of the npx cache and source.
  fs.rmSync(path.join(temp, 'cache'), { recursive: true, force: true });
  const packageName = require('../package.json').name;
  const globalRoot = process.platform === 'win32' ? path.join(prefix, 'node_modules') : path.join(prefix, 'lib', 'node_modules');
  const installedRoot = path.join(globalRoot, packageName);
  assert.equal(fs.lstatSync(installedRoot).isSymbolicLink(), false);
  for (const command of ['s3get', 's3push']) {
    const shim = process.platform === 'win32' ? path.join(prefix, `${command}.cmd`) : path.join(prefix, 'bin', command);
    assert.ok(fs.existsSync(shim), `Missing global command: ${shim}`);
    const result = execute(process.execPath, [path.join(installedRoot, 'npm', `${command}.cjs`), '--help'], { cwd: temp });
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout + result.stderr, /-concurrency N/);
  }
  const unifiedShim = process.platform === 'win32' ? path.join(prefix, 's3flow.cmd') : path.join(prefix, 'bin', 's3flow');
  assert.ok(fs.existsSync(unifiedShim), 'Missing global s3flow entrypoint');
  for (const subcommand of ['get', 'push']) {
    const result = execute(process.execPath, [path.join(installedRoot, 'npm', 'cli.cjs'), subcommand, '--help'], { cwd: temp });
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /-concurrency N/);
  }
  console.log(`npm package smoke tests passed on ${process.platform}/${process.arch}`);
} finally {
  fs.rmSync(temp, { recursive: true, force: true });
}

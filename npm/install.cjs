'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { binaryPath } = require('./run.cjs');

function parseArgs(args) {
  if (args.length === 0) return {};
  if (args.length === 1 && ['--help', '-h'].includes(args[0])) return { help: true };
  if (args.length === 2 && args[0] === '--prefix' && args[1] && !args[1].startsWith('-')) {
    return { prefix: path.resolve(args[1]) };
  }
  throw new Error('Usage: s3flow install [--prefix DIRECTORY]');
}

function install(args) {
  let temp;
  try {
    const options = parseArgs(args);
    if (options.help) {
      console.log('Usage: s3flow install [--prefix DIRECTORY]\n\nInstall both commands into the npm global prefix.\nUse --prefix to select a writable installation directory.');
      return;
    }
    const npmCLI = process.env.npm_execpath;
    if (!npmCLI || !fs.existsSync(npmCLI)) {
      throw new Error('Run this installer through npx so it can locate npm.');
    }
    // Check both binaries before changing the global installation.
    for (const command of ['s3get', 's3push']) fs.accessSync(binaryPath(command), fs.constants.R_OK);

    temp = fs.mkdtempSync(path.join(os.tmpdir(), 's3flow-install-'));
    const stage = path.join(temp, 'package');
    const native = path.join(stage, 'npm', 'native', `${process.platform}-${process.arch}`);
    fs.mkdirSync(native, { recursive: true });
    const root = path.resolve(__dirname, '..');
    for (const file of ['package.json', 'README.md', 'LICENSE']) fs.copyFileSync(path.join(root, file), path.join(stage, file));
    for (const file of fs.readdirSync(__dirname).filter(name => name.endsWith('.cjs'))) {
      fs.copyFileSync(path.join(__dirname, file), path.join(stage, 'npm', file));
    }
    for (const command of ['s3get', 's3push']) {
      const source = binaryPath(command);
      fs.copyFileSync(source, path.join(native, path.basename(source)));
      fs.chmodSync(path.join(native, path.basename(source)), 0o755);
    }

    function npm(args, cwd = stage) {
      const result = spawnSync(process.execPath, [npmCLI, ...args], { cwd, stdio: 'inherit', shell: false });
      if (result.error) throw result.error;
      if (result.status !== 0) throw new Error(`npm exited with ${result.status ?? result.signal}`);
    }
    // Pack first: a global link to the npx cache would break after cache cleanup.
    npm(['pack', '--ignore-scripts', '--pack-destination', temp]);
    const archives = fs.readdirSync(temp).filter(name => name.endsWith('.tgz'));
    if (archives.length !== 1) throw new Error('Unable to locate the installation package.');
    const prefixArgs = options.prefix ? ['--prefix', options.prefix] : [];
    npm(['install', '--global', '--ignore-scripts', '--no-audit', '--no-fund', ...prefixArgs, path.join(temp, archives[0])], temp);
    const prefix = options.prefix || (() => {
      const result = spawnSync(process.execPath, [npmCLI, 'prefix', '--global'], { encoding: 'utf8', shell: false });
      return result.status === 0 ? result.stdout.trim() : '';
    })();
    console.log('Installed s3get and s3push. Run: s3get --help / s3push --help');
    if (prefix) console.log(`Command directory (must be on PATH): ${process.platform === 'win32' ? prefix : path.join(prefix, 'bin')}`);
  } catch (error) {
    console.error(`s3flow install: ${error.message}`);
    console.error('If the global directory is not writable, retry with install --prefix <writable-directory>.');
    process.exitCode = 1;
  } finally {
    if (temp) fs.rmSync(temp, { recursive: true, force: true });
  }
}

module.exports = { parseArgs, install };

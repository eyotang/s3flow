'use strict';

const path = require('node:path');
const { spawn } = require('node:child_process');
const { constants } = require('node:os');

function binaryPath(command, platform = process.platform, arch = process.arch) {
  if (command !== 's3flow') {
    throw new Error(`Unknown command: ${command}. Use get or push (aliases: s3get, s3push).`);
  }
  if (!['win32', 'darwin', 'linux'].includes(platform) || !['x64', 'arm64'].includes(arch)) {
    throw new Error(`Unsupported platform: ${platform}/${arch}. Supported: Windows, macOS, Linux on x64 or arm64.`);
  }
  return path.join(__dirname, 'native', `${platform}-${arch}`, command + (platform === 'win32' ? '.exe' : ''));
}

function run(command, args) {
  let executable;
  try {
    if (command === 's3get' || command === 's3push') {
      args = [command === 's3get' ? 'get' : 'push', ...args];
    } else if (command !== 's3flow') {
      throw new Error(`Unknown command: ${command}`);
    }
    executable = binaryPath('s3flow');
  } catch (error) {
    console.error(`s3flow: ${error.message}`);
    process.exitCode = 1;
    return;
  }

  // Inherit the terminal for Bubble Tea and forward signals sent to the wrapper.
  const child = spawn(executable, args, { stdio: 'inherit', shell: false });
  const handlers = new Map();
  for (const signal of ['SIGINT', 'SIGTERM']) {
    const handler = () => child.kill(signal);
    handlers.set(signal, handler);
    process.on(signal, handler);
  }
  child.on('error', (error) => {
    console.error(`s3flow: Cannot start ${command}: ${error.message}. Reinstall the release npm package.`);
  });
  child.on('close', (code, signal) => {
    for (const [name, handler] of handlers) process.removeListener(name, handler);
    process.exitCode = code ?? (signal ? 128 + (constants.signals[signal] || 1) : 1);
  });
}

module.exports = { binaryPath, run };

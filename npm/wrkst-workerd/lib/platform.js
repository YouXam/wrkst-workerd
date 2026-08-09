'use strict';

const fs = require('node:fs');
const path = require('node:path');

const packages = {
  'darwin-arm64': '@wrkst/workerd-darwin-arm64',
  'linux-x64': '@wrkst/workerd-linux-64',
};

function packageForCurrentPlatform() {
  const platform = `${process.platform}-${process.arch}`;
  const packageName = packages[platform];
  if (!packageName) throw new Error(`Unsupported platform: ${platform}`);
  return packageName;
}

function binaryPath() {
  const packageName = packageForCurrentPlatform();
  try {
    return require.resolve(`${packageName}/bin/wrkst-workerd`);
  } catch (error) {
    const fallbackPath = downloadedBinaryPath();
    if (fs.existsSync(fallbackPath)) return fallbackPath;
    throw error;
  }
}

function downloadedBinaryPath() {
  const packageName = packageForCurrentPlatform().replace('/', '-');
  return path.join(__dirname, '..', `downloaded-${packageName}`);
}

module.exports = {
  binaryPath,
  downloadedBinaryPath,
  packageForCurrentPlatform,
};

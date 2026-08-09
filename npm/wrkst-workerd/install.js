'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const {
  binaryPath,
  downloadedBinaryPath,
  packageForCurrentPlatform,
} = require('./lib/platform');

const targetPath = path.join(__dirname, 'bin', 'wrkst-workerd');
const temporaryPath = `${targetPath}.native`;
let sourcePath;

try {
  sourcePath = binaryPath();
} catch {
  const packageName = packageForCurrentPlatform();
  const version = require('./package.json').version;
  const installDirectory = fs.mkdtempSync(
    path.join(os.tmpdir(), 'wrkst-workerd-')
  );

  try {
    execFileSync(
      'npm',
      [
        'install',
        '--ignore-scripts',
        '--no-audit',
        '--no-save',
        '--prefix',
        installDirectory,
        `${packageName}@${version}`,
      ],
      {
        env: { ...process.env, npm_config_global: undefined },
        stdio: 'inherit',
      }
    );
    sourcePath = path.join(
      installDirectory,
      'node_modules',
      ...packageName.split('/'),
      'bin',
      'wrkst-workerd'
    );
    fs.copyFileSync(sourcePath, downloadedBinaryPath());
    fs.chmodSync(downloadedBinaryPath(), 0o755);
    sourcePath = downloadedBinaryPath();
  } finally {
    fs.rmSync(installDirectory, { recursive: true, force: true });
  }
}

if (!/\byarn\//.test(process.env.npm_config_user_agent ?? '')) {
  try {
    fs.rmSync(temporaryPath, { force: true });
    fs.linkSync(sourcePath, temporaryPath);
    fs.renameSync(temporaryPath, targetPath);
  } catch {
    fs.rmSync(temporaryPath, { force: true });
  }
}

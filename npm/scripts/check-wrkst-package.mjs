import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const packageDirectory = process.argv[2];
if (!packageDirectory) {
  throw new Error('usage: check-wrkst-package.mjs <directory>');
}

const packageJson = JSON.parse(
  fs.readFileSync(path.join(packageDirectory, 'package.json'), 'utf8')
);
const expectedFiles = {
  '@wrkst/workerd': [
    'NOTICE',
    'bin/wrkst-workerd',
    'install.js',
    'lib/platform.js',
    'package.json',
    'workerd/server/workerd-admin.capnp',
    'workerd/server/workerd.capnp',
  ],
  '@wrkst/workerd-darwin-arm64': ['bin/wrkst-workerd', 'package.json'],
  '@wrkst/workerd-linux-64': ['bin/wrkst-workerd', 'package.json'],
}[packageJson.name];

if (!expectedFiles) throw new Error(`unexpected package: ${packageJson.name}`);

const packed = JSON.parse(
  execFileSync('npm', ['pack', '--dry-run', '--json'], {
    cwd: packageDirectory,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'inherit'],
  })
);
const actualFiles = packed[0].files.map((file) => file.path).sort();

if (JSON.stringify(actualFiles) !== JSON.stringify(expectedFiles)) {
  throw new Error(
    `${packageJson.name} contains unexpected files: ${actualFiles.join(', ')}`
  );
}

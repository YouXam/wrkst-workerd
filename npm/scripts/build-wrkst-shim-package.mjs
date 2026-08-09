import fs from 'node:fs';
import path from 'node:path';

const packageDirectory = process.argv[2];
const version = process.argv[3];

if (!packageDirectory || !version) {
  throw new Error('usage: build-wrkst-shim-package.mjs <directory> <version>');
}

const packagePath = path.join(packageDirectory, 'package.json');
const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));

packageJson.version = version;
packageJson.optionalDependencies = {
  '@wrkst/workerd-darwin-arm64': version,
  '@wrkst/workerd-linux-64': version,
};

fs.writeFileSync(packagePath, `${JSON.stringify(packageJson, null, 2)}\n`);
const schemaDirectory = path.join(packageDirectory, 'workerd', 'server');
fs.mkdirSync(schemaDirectory, { recursive: true });
fs.copyFileSync(
  path.join('src', 'workerd', 'server', 'workerd.capnp'),
  path.join(schemaDirectory, 'workerd.capnp')
);
fs.copyFileSync(
  path.join('src', 'workerd', 'server', 'workerd-admin.capnp'),
  path.join(schemaDirectory, 'workerd-admin.capnp')
);

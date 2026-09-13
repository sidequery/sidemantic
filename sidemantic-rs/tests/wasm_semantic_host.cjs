// Exercise the generated public JS module, including its exception boundary.
const fs = require('node:fs');
const wasm = require(process.argv[2]);
const request = JSON.parse(fs.readFileSync(0, 'utf8'));
try {
  const result = wasm[request.method](...request.args);
  process.stdout.write(JSON.stringify({result}));
} catch (error) {
  process.stdout.write(JSON.stringify({error: String(error)}));
}

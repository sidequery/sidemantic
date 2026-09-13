// Exercise the generated public JS module, including its exception boundary.
const fs = require('node:fs');
const wasm = require(process.argv[2]);
for (const method of ['compile', 'validate', 'rewrite']) {
  if (typeof wasm[`wasm_${method}_with_semantic_input`] !== 'function') {
    throw new Error(`Missing SemanticInput export: ${method}`);
  }
}
if (typeof wasm.wasm_rewrite_with_semantic_input_context !== 'function') {
  throw new Error('Missing SemanticInput context rewrite export');
}
const request = JSON.parse(fs.readFileSync(0, 'utf8'));
try {
  const result = wasm[request.method](...request.args);
  process.stdout.write(JSON.stringify({result}));
} catch (error) {
  process.stdout.write(JSON.stringify({error: String(error)}));
}

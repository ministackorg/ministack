const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');

const codeDir = process.env._LAMBDA_CODE_DIR || '/var/task';
const modPath  = process.env._LAMBDA_HANDLER_MODULE;
const fnName   = process.env._LAMBDA_HANDLER_FUNC;

// Prepend layer dirs to NODE_PATH
const layerDirs = (process.env._LAMBDA_LAYERS_DIRS || '').split(path.delimiter).filter(Boolean);
const nodePaths = layerDirs.map(d => path.join(d, 'nodejs', 'node_modules'))
                           .concat(layerDirs)
                           .concat([path.join(codeDir, 'node_modules'), codeDir]);
module.paths.unshift(...nodePaths);

const context = {
  functionName:       process.env.AWS_LAMBDA_FUNCTION_NAME || '',
  memoryLimitInMB:    process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || '128',
  invokedFunctionArn: process.env._LAMBDA_FUNCTION_ARN || '',
  awsRequestId:       process.env.AWS_LAMBDA_LOG_STREAM_NAME || '',
  logGroupName:       '/aws/lambda/' + (process.env.AWS_LAMBDA_FUNCTION_NAME || ''),
  logStreamName:      process.env.AWS_LAMBDA_LOG_STREAM_NAME || '',
  getRemainingTimeInMillis: () => parseFloat(process.env._LAMBDA_TIMEOUT || '3') * 1000,
};

let input = '';
process.stdin.on('data', d => input += d);
process.stdin.on('end', async () => {
  const event = JSON.parse(input);
  const fullPath = path.resolve(codeDir, modPath);
  let mod;
  let resolvedPath;
  try {
    resolvedPath = require.resolve(fullPath);
    mod = require(resolvedPath);
  } catch (reqErr) {
    if (reqErr.code === 'ERR_REQUIRE_ESM' && resolvedPath) {
      mod = await import(pathToFileURL(resolvedPath).href);
    } else if (reqErr.code === 'MODULE_NOT_FOUND') {
      const mjsPath = fullPath + '.mjs';
      const missingHandlerEntry =
        (reqErr.message && reqErr.message.includes("'" + fullPath + "'")) ||
        (resolvedPath && reqErr.message && reqErr.message.includes("'" + resolvedPath + "'"));
      if (missingHandlerEntry && fs.existsSync(mjsPath)) {
        mod = await import(pathToFileURL(mjsPath).href);
      } else {
        throw reqErr;
      }
    } else {
      throw reqErr;
    }
  }
  const handler = mod[fnName] || (mod.default && mod.default[fnName]) || mod.default;
  if (typeof handler !== 'function') {
    process.stderr.write(
      "Handler '" + fnName + "' in module '" + modPath + "' is undefined or not a function"
    );
    process.exit(1);
  }
  Promise.resolve(handler(event, context)).then(result => {
    if (result !== undefined) process.stdout.write(JSON.stringify(result));
  }).catch(err => {
    process.stderr.write(String(err.stack || err));
    process.exit(1);
  });
});
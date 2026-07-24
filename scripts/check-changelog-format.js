#!/usr/bin/env node

const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const changelogPath = 'CHANGELOG.md';
const args = process.argv.slice(2);

function parseArgs(argv) {
  const options = { baseRef: null, staged: false };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === '--staged') {
      options.staged = true;
    } else if (arg === '--base') {
      options.baseRef = argv[index + 1];
      index += 1;
    } else if (arg.startsWith('--base=')) {
      options.baseRef = arg.slice('--base='.length);
    }
  }

  return options;
}

function runGit(commandArgs) {
  return execFileSync('git', commandArgs, {
    cwd: process.cwd(),
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

function getDiffOutput(options) {
  const commandArgs = ['diff', '--unified=0', '--no-color', '--diff-filter=AM'];

  if (options.staged) {
    commandArgs.push('--cached');
  }

  if (options.baseRef) {
    commandArgs.push(`${options.baseRef}...HEAD`);
  }

  commandArgs.push('--', changelogPath);

  try {
    return runGit(commandArgs);
  } catch (error) {
    if (error.status === 128) {
      const stderr = error.stderr ? error.stderr.toString() : '';
      throw new Error(`Unable to read changelog diff: ${stderr.trim() || error.message}`);
    }

    throw error;
  }
}

function isAllowedCategory(content) {
  return ['Added', 'Changed', 'Fixed', 'Removed', 'Deprecated'].includes(content);
}

function validateChangelogLine(content) {
  const trimmed = content.trim();

  if (!trimmed) {
    return null;
  }

  if (trimmed.startsWith('### ')) {
    const category = trimmed.slice(4).trim();
    if (!isAllowedCategory(category)) {
      return `Unexpected changelog category "${category}". Use one of: Added, Changed, Fixed, Removed, Deprecated.`;
    }

    return null;
  }

  if (trimmed.startsWith('- ')) {
    const entryPattern = /^-\s+.+\s\(#\d+\)$/;
    if (!entryPattern.test(trimmed)) {
      return 'Expected changelog entries to use the format "- Short description (#issue)".';
    }

    return null;
  }

  return null;
}

function collectChangelogValidationErrors(diffOutput) {
  const errors = [];
  const lines = diffOutput.split(/\r?\n/);

  for (const line of lines) {
    if (!line.startsWith('+') || line.startsWith('+++')) {
      continue;
    }

    const content = line.slice(1);
    const error = validateChangelogLine(content);
    if (error) {
      errors.push({ line, error });
    }
  }

  return errors;
}

function main() {
  const resolvedPath = path.resolve(changelogPath);
  if (!fs.existsSync(resolvedPath)) {
    console.log('No CHANGELOG.md found; skipping changelog validation.');
    return;
  }

  const options = parseArgs(args);
  const diffOutput = getDiffOutput(options);

  if (!diffOutput.trim()) {
    console.log('No changelog changes detected; skipping validation.');
    return;
  }

  const errors = collectChangelogValidationErrors(diffOutput);
  if (errors.length > 0) {
    console.error('Changelog format validation failed:');
    for (const entry of errors) {
      console.error(`- ${entry.error}`);
      console.error(`  ${entry.line}`);
    }
    process.exit(1);
  }

  console.log('Changelog format validation passed.');
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}

module.exports = {
  collectChangelogValidationErrors,
  validateChangelogLine,
};

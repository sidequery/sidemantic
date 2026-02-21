import { describe, expect, test } from 'bun:test';
import {
  SIDEMANTIC_INSTALL_COMMAND,
  SIDEMANTIC_SQL_FILE_GLOB,
  SIDEMANTIC_SQL_LANGUAGE_ID,
  buildServerCommand,
  getStartupFailure,
} from '../src/lspConfig';

describe('lspConfig', () => {
  test('buildServerCommand uses sidemantic lsp args', () => {
    expect(buildServerCommand('sidemantic')).toEqual({
      command: 'sidemantic',
      args: ['lsp'],
    });
  });

  test('marks ENOENT startup errors as missing CLI', () => {
    const failure = getStartupFailure(new Error('spawn sidemantic ENOENT'));
    expect(failure.missingCli).toBe(true);
    expect(failure.message).toContain('ENOENT');
  });

  test('marks not found startup errors as missing CLI', () => {
    const failure = getStartupFailure(new Error('command not found: sidemantic'));
    expect(failure.missingCli).toBe(true);
  });

  test('keeps unknown startup errors as non-cli failures', () => {
    const failure = getStartupFailure(new Error('connection refused'));
    expect(failure.missingCli).toBe(false);
    expect(failure.message).toBe('connection refused');
  });

  test('handles non-Error startup failures safely', () => {
    const failure = getStartupFailure({ message: 'bad object' });
    expect(failure.missingCli).toBe(false);
    expect(failure.message).toBe('Unknown error');
  });

  test('exports stable extension constants', () => {
    expect(SIDEMANTIC_SQL_FILE_GLOB).toBe('**/*.sidemantic.sql');
    expect(SIDEMANTIC_SQL_LANGUAGE_ID).toBe('sidemantic-sql');
    expect(SIDEMANTIC_INSTALL_COMMAND).toBe('uv pip install sidemantic[lsp]');
  });
});

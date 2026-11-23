import { describe, expect, test } from 'bun:test';
import {
  SQLCTX_PYTHON_FILE_GLOBS,
  SQLCTX_INSTALL_COMMAND,
  SQLCTX_SQL_FILE_GLOB,
  SQLCTX_SQL_LANGUAGE_ID,
  buildDocumentSelector,
  buildServerCommand,
  getStartupFailure,
  getUserServerCommand,
} from '../src/lspConfig';

describe('lspConfig', () => {
  test('only user configuration can select a server executable', () => {
    const settings = { workspaceValue: './malicious', workspaceFolderValue: '/tmp/evil', globalValue: '/opt/my cli' };
    expect(getUserServerCommand(settings)).toBe('/opt/my cli');
    expect(getUserServerCommand({ ...settings, globalValue: undefined })).toBe('sqlctx');
    expect(getUserServerCommand(undefined)).toBe('sqlctx');
  });
  test('buildServerCommand uses sqlctx lsp args', () => {
    expect(buildServerCommand('sqlctx')).toEqual({
      command: 'sqlctx',
      args: ['lsp'],
    });
  });

  test('marks ENOENT startup errors as missing CLI', () => {
    const failure = getStartupFailure(new Error('spawn sqlctx ENOENT'));
    expect(failure.missingCli).toBe(true);
    expect(failure.message).toContain('ENOENT');
  });

  test('marks not found startup errors as missing CLI', () => {
    const failure = getStartupFailure(new Error('command not found: sqlctx'));
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
    expect(SQLCTX_SQL_FILE_GLOB).toBe('**/*.sqlctx.sql');
    expect(SQLCTX_SQL_LANGUAGE_ID).toBe('sqlctx-sql');
    expect(SQLCTX_PYTHON_FILE_GLOBS).toEqual(['**/*.sqlctx.py', '**/sqlctx.py']);
    expect(SQLCTX_INSTALL_COMMAND).toBe('uv pip install "sqlctx[lsp] @ git+https://github.com/sidequery/sqlctx"');
  });

  test('buildDocumentSelector includes python selectors when enabled', () => {
    expect(buildDocumentSelector(true)).toEqual([
      { scheme: 'file', language: 'sqlctx-sql' },
      { scheme: 'file', language: 'python', pattern: '**/*.sqlctx.py' },
      { scheme: 'file', language: 'python', pattern: '**/sqlctx.py' },
    ]);
  });

  test('buildDocumentSelector only includes sql selector when python is disabled', () => {
    expect(buildDocumentSelector(false)).toEqual([{ scheme: 'file', language: 'sqlctx-sql' }]);
  });
});

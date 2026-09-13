import { expect, mock, test } from 'bun:test';

const workspace = {
  isTrusted: false,
  getConfiguration: mock(() => ({
    get: (_name: string, fallback: unknown) => fallback,
    inspect: () => ({ globalValue: '/opt/user/sidemantic', workspaceValue: '/tmp/attacker' }),
  })),
  createFileSystemWatcher: mock(() => ({})),
};
const start = mock(async () => {});
const construct = mock((_id: string, _name: string, _server: unknown) => {});
mock.module('vscode', () => ({ workspace }));
mock.module('vscode-languageclient/node', () => ({
  TransportKind: { stdio: 0 },
  LanguageClient: class {
    constructor(id: string, name: string, server: unknown) {
      construct(id, name, server);
    }
    start = start;
  },
}));
const { activate } = await import('../src/extension');

test('activation refuses untrusted workspaces and honors only the user executable after trust', async () => {
  const context = { subscriptions: [] };
  await activate(context as never);
  expect(workspace.getConfiguration).not.toHaveBeenCalled();
  expect(workspace.createFileSystemWatcher).not.toHaveBeenCalled();
  expect(start).not.toHaveBeenCalled();
  workspace.isTrusted = true;
  await activate(context as never);
  expect(construct.mock.calls[0][2]).toEqual({ command: '/opt/user/sidemantic', args: ['lsp'], transport: 0 });
  expect(start).toHaveBeenCalledTimes(1);
});

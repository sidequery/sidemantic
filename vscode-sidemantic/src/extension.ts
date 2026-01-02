import * as vscode from 'vscode';
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from 'vscode-languageclient/node';

let client: LanguageClient | undefined;

export async function activate(context: vscode.ExtensionContext) {
  const config = vscode.workspace.getConfiguration('sidemantic');

  if (!config.get<boolean>('lsp.enabled', true)) {
    return;
  }

  const command = config.get<string>('lsp.path', 'sidemantic');

  const serverOptions: ServerOptions = {
    command,
    args: ['lsp'],
    transport: TransportKind.stdio,
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: 'file', language: 'sidemantic-sql' }],
    synchronize: {
      fileEvents: vscode.workspace.createFileSystemWatcher('**/*.sidemantic.sql'),
    },
  };

  client = new LanguageClient(
    'sidemantic',
    'Sidemantic Language Server',
    serverOptions,
    clientOptions
  );

  try {
    await client.start();
  } catch (error) {
    const message =
      error instanceof Error ? error.message : 'Unknown error';

    if (message.includes('ENOENT') || message.includes('not found')) {
      vscode.window
        .showErrorMessage(
          `Sidemantic CLI not found. Install with: uv pip install sidemantic[lsp]`,
          'Copy Install Command'
        )
        .then((selection) => {
          if (selection === 'Copy Install Command') {
            vscode.env.clipboard.writeText('uv pip install sidemantic[lsp]');
            vscode.window.showInformationMessage('Install command copied to clipboard');
          }
        });
    } else {
      vscode.window.showErrorMessage(`Failed to start Sidemantic LSP: ${message}`);
    }
  }
}

export function deactivate(): Thenable<void> | undefined {
  if (!client) {
    return undefined;
  }
  return client.stop();
}

import * as vscode from 'vscode';
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from 'vscode-languageclient/node';
import {
  SIDEMANTIC_INSTALL_COMMAND,
  SIDEMANTIC_SQL_FILE_GLOB,
  SIDEMANTIC_SQL_LANGUAGE_ID,
  buildServerCommand,
  getStartupFailure,
} from './lspConfig';

let client: LanguageClient | undefined;

export async function activate(context: vscode.ExtensionContext) {
  const config = vscode.workspace.getConfiguration('sidemantic');

  if (!config.get<boolean>('lsp.enabled', true)) {
    return;
  }

  const command = config.get<string>('lsp.path', 'sidemantic');

  const watcher = vscode.workspace.createFileSystemWatcher(SIDEMANTIC_SQL_FILE_GLOB);
  context.subscriptions.push(watcher);

  const serverOptions: ServerOptions = {
    ...buildServerCommand(command),
    transport: TransportKind.stdio,
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: 'file', language: SIDEMANTIC_SQL_LANGUAGE_ID }],
    synchronize: {
      fileEvents: watcher,
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
    const failure = getStartupFailure(error);

    if (failure.missingCli) {
      vscode.window
        .showErrorMessage(
          `Sidemantic CLI not found. Install with: ${SIDEMANTIC_INSTALL_COMMAND}`,
          'Copy Install Command'
        )
        .then((selection) => {
          if (selection === 'Copy Install Command') {
            vscode.env.clipboard.writeText(SIDEMANTIC_INSTALL_COMMAND);
            vscode.window.showInformationMessage('Install command copied to clipboard');
          }
        });
    } else {
      vscode.window.showErrorMessage(`Failed to start Sidemantic LSP: ${failure.message}`);
    }
  }
}

export function deactivate(): Thenable<void> | undefined {
  if (!client) {
    return undefined;
  }
  return client.stop();
}

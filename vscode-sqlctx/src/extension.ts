import * as vscode from 'vscode';
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from 'vscode-languageclient/node';
import {
  SQLCTX_PYTHON_FILE_GLOBS,
  SQLCTX_INSTALL_COMMAND,
  SQLCTX_SQL_FILE_GLOB,
  buildDocumentSelector,
  buildServerCommand,
  getUserServerCommand,
  getStartupFailure,
} from './lspConfig';

let client: LanguageClient | undefined;

export async function activate(context: vscode.ExtensionContext) {
  if (!vscode.workspace.isTrusted) {
    return;
  }
  const config = vscode.workspace.getConfiguration('sqlctx');

  if (!config.get<boolean>('lsp.enabled', true)) {
    return;
  }

  const command = getUserServerCommand(config.inspect<string>('lsp.path'));
  const pythonEnabled = config.get<boolean>('lsp.python.enabled', true);

  const watchers = [vscode.workspace.createFileSystemWatcher(SQLCTX_SQL_FILE_GLOB)];
  if (pythonEnabled) {
    for (const glob of SQLCTX_PYTHON_FILE_GLOBS) {
      watchers.push(vscode.workspace.createFileSystemWatcher(glob));
    }
  }
  for (const watcher of watchers) {
    context.subscriptions.push(watcher);
  }

  const serverOptions: ServerOptions = {
    ...buildServerCommand(command),
    transport: TransportKind.stdio,
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: buildDocumentSelector(pythonEnabled),
    synchronize: {
      fileEvents: watchers,
    },
  };

  client = new LanguageClient(
    'sqlctx',
    'sqlctx Language Server',
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
          `sqlctx CLI not found. Install with: ${SQLCTX_INSTALL_COMMAND}`,
          'Copy Install Command'
        )
        .then((selection) => {
          if (selection === 'Copy Install Command') {
            vscode.env.clipboard.writeText(SQLCTX_INSTALL_COMMAND);
            vscode.window.showInformationMessage('Install command copied to clipboard');
          }
        });
    } else {
      vscode.window.showErrorMessage(`Failed to start sqlctx LSP: ${failure.message}`);
    }
  }
}

export function deactivate(): Thenable<void> | undefined {
  if (!client) {
    return undefined;
  }
  return client.stop();
}

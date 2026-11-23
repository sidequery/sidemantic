export const SQLCTX_SQL_FILE_GLOB = '**/*.sqlctx.sql';
export const SQLCTX_SQL_LANGUAGE_ID = 'sqlctx-sql';
export const SQLCTX_PYTHON_FILE_GLOBS = ['**/*.sqlctx.py', '**/sqlctx.py'] as const;
export const PYTHON_LANGUAGE_ID = 'python';
export const SQLCTX_INSTALL_COMMAND = 'uv pip install "sqlctx[lsp] @ git+https://github.com/sidequery/sqlctx"';

export type ServerCommand = {
  command: string;
  args: string[];
};

export type StartupFailure = {
  message: string;
  missingCli: boolean;
};

export type DocumentSelectorEntry = {
  scheme: 'file';
  language: string;
  pattern?: string;
};

export function buildServerCommand(command: string): ServerCommand {
  return {
    command,
    args: ['lsp'],
  };
}

export function getUserServerCommand(setting: { globalValue?: string } | undefined): string {
  // Workspace settings are project input and must never select an executable.
  return setting?.globalValue ?? 'sqlctx';
}

export function buildDocumentSelector(enablePython: boolean): DocumentSelectorEntry[] {
  const selectors: DocumentSelectorEntry[] = [
    {
      scheme: 'file',
      language: SQLCTX_SQL_LANGUAGE_ID,
    },
  ];

  if (!enablePython) {
    return selectors;
  }

  for (const pattern of SQLCTX_PYTHON_FILE_GLOBS) {
    selectors.push({
      scheme: 'file',
      language: PYTHON_LANGUAGE_ID,
      pattern,
    });
  }

  return selectors;
}

export function getStartupFailure(error: unknown): StartupFailure {
  const message = error instanceof Error ? error.message : 'Unknown error';
  const normalized = message.toLowerCase();

  return {
    message,
    missingCli: normalized.includes('enoent') || normalized.includes('not found'),
  };
}

export const SIDEMANTIC_SQL_FILE_GLOB = '**/*.sidemantic.sql';
export const SIDEMANTIC_SQL_LANGUAGE_ID = 'sidemantic-sql';
export const SIDEMANTIC_PYTHON_FILE_GLOBS = ['**/*.sidemantic.py', '**/sidemantic.py'] as const;
export const PYTHON_LANGUAGE_ID = 'python';
export const SIDEMANTIC_INSTALL_COMMAND = 'uv pip install sidemantic[lsp]';

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

export function buildDocumentSelector(enablePython: boolean): DocumentSelectorEntry[] {
  const selectors: DocumentSelectorEntry[] = [
    {
      scheme: 'file',
      language: SIDEMANTIC_SQL_LANGUAGE_ID,
    },
  ];

  if (!enablePython) {
    return selectors;
  }

  for (const pattern of SIDEMANTIC_PYTHON_FILE_GLOBS) {
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

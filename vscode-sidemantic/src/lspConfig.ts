export const SIDEMANTIC_SQL_FILE_GLOB = '**/*.sidemantic.sql';
export const SIDEMANTIC_SQL_LANGUAGE_ID = 'sidemantic-sql';
export const SIDEMANTIC_INSTALL_COMMAND = 'uv pip install sidemantic[lsp]';

export type ServerCommand = {
  command: string;
  args: string[];
};

export type StartupFailure = {
  message: string;
  missingCli: boolean;
};

export function buildServerCommand(command: string): ServerCommand {
  return {
    command,
    args: ['lsp'],
  };
}

export function getStartupFailure(error: unknown): StartupFailure {
  const message = error instanceof Error ? error.message : 'Unknown error';
  const normalized = message.toLowerCase();

  return {
    message,
    missingCli: normalized.includes('enoent') || normalized.includes('not found'),
  };
}

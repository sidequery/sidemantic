#!/usr/bin/env bash
set -euo pipefail

SKILL_NAME="sidemantic-modeler"
REPO="sidequery/sidemantic"
BRANCH="main"
REMOTE_DIR="skills/$SKILL_NAME"
BASE_URL="https://raw.githubusercontent.com/$REPO/$BRANCH/$REMOTE_DIR"

AGENTS_DIR="$HOME/.agents/skills/$SKILL_NAME"
CLAUDE_DIR="$HOME/.claude/skills/$SKILL_NAME"

echo "Installing $SKILL_NAME skill..."

# Create target directory
mkdir -p "$AGENTS_DIR/references"

# Download SKILL.md and reference files
curl -fsSL "$BASE_URL/SKILL.md" -o "$AGENTS_DIR/SKILL.md"
for ref in generation.md migration.md patterns.md validation.md yaml-schema.md; do
  curl -fsSL "$BASE_URL/references/$ref" -o "$AGENTS_DIR/references/$ref"
done

# Symlink into Claude skills
mkdir -p "$HOME/.claude/skills"
if [ -L "$CLAUDE_DIR" ]; then
  rm "$CLAUDE_DIR"
elif [ -d "$CLAUDE_DIR" ]; then
  echo "Warning: $CLAUDE_DIR exists and is not a symlink. Skipping symlink creation."
  echo "Skill files installed to $AGENTS_DIR"
  exit 0
fi
ln -s "$AGENTS_DIR" "$CLAUDE_DIR"

echo "Installed to $AGENTS_DIR"
echo "Symlinked to $CLAUDE_DIR"
echo "Done. The skill is now available in Claude Code and compatible agents."

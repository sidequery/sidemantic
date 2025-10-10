#!/usr/bin/env python3
"""Generate llms.txt for the Sidemantic documentation site.

This script creates an llms.txt file that helps LLMs understand the documentation
structure and content by reading from _quarto.yml and page files.
"""

import re
from pathlib import Path

import yaml


def extract_title_and_description(qmd_path: Path) -> tuple[str, str]:
    """Extract title and description from a qmd file."""
    content = qmd_path.read_text()

    # Parse YAML frontmatter
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            try:
                frontmatter = yaml.safe_load(parts[1])
                title = frontmatter.get("title", "")
                subtitle = frontmatter.get("subtitle", "")

                # If subtitle exists, use it as description
                if subtitle:
                    return title, subtitle

                # Otherwise get first paragraph after frontmatter
                body = parts[2].strip()
                lines = []
                in_code = False
                skip_patterns = [":::", "---", "```"]

                for line in body.split("\n"):
                    stripped = line.strip()

                    # Skip code blocks
                    if stripped.startswith("```"):
                        in_code = not in_code
                        continue

                    if in_code:
                        continue

                    # Skip quarto directives and headers
                    if any(stripped.startswith(p) for p in skip_patterns) or stripped.startswith("#"):
                        continue

                    # Skip empty lines
                    if not stripped:
                        if lines:  # Stop after first paragraph
                            break
                        continue

                    # Clean markdown formatting
                    clean = re.sub(r'\*\*([^*]+)\*\*', r'\1', stripped)
                    clean = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', clean)
                    lines.append(clean)

                    # Stop if we have enough text
                    if len(" ".join(lines)) > 150:
                        break

                description = " ".join(lines)
                # Truncate at sentence boundary if too long
                if len(description) > 200:
                    # Try to cut at last sentence
                    sentences = re.split(r'[.!?]\s+', description)
                    description = sentences[0]
                    if not description.endswith(('.', '!', '?')):
                        description += '.'

                return title, description
            except Exception:
                pass

    return "", ""


def generate_llms_txt():
    """Generate llms.txt from quarto config and page content."""

    docs_dir = Path(__file__).parent
    config_path = docs_dir / "_quarto.yml"

    # Load quarto config
    config = yaml.safe_load(config_path.read_text())

    # Start building content
    lines = ["# Sidemantic", ""]

    # Get description from index.qmd
    index_path = docs_dir / "index.qmd"
    if index_path.exists():
        _, description = extract_title_and_description(index_path)
        if description:
            lines.append(f"> {description}")
            lines.append("")

    # Add intro from index
    lines.append("Sidemantic is a semantic layer that works with your existing data warehouse. Define metrics, dimensions, and relationships in YAML (or import from Cube, dbt, Looker, etc.), then query them using SQL or Python.")
    lines.append("")
    lines.append("Key features:")
    lines.append("- **Governed calculations**: Define metrics once, query consistently everywhere")
    lines.append("- **Accurate by design**: Prevents join fan-out, incorrect aggregations, and double-counting")
    lines.append("- **Smart automation**: Automatic joins, dependency detection, multi-hop relationships")
    lines.append("- **Rich metric types**: Aggregations, ratios, time comparisons, funnels, cumulative metrics")
    lines.append("- **Format compatibility**: Import from Cube, MetricFlow (dbt), LookML (Looker), Hex, Rill, Superset, Omni")
    lines.append("")

    # Process navbar structure
    navbar = config.get("website", {}).get("navbar", {})
    left_items = navbar.get("left", [])

    base_url = "https://docs.sidemantic.com"

    for item in left_items:
        if "menu" in item:
            # Menu item with submenu
            section_name = item.get("text", "")
            lines.append(f"## {section_name}")
            lines.append("")

            for subitem in item["menu"]:
                href = subitem.get("href", "")
                text = subitem.get("text", "")

                if href and href.endswith(".qmd"):
                    # Convert .qmd to .html for URLs
                    url_path = href.replace(".qmd", ".html")

                    # Read description from file
                    file_path = docs_dir / href
                    description = ""
                    if file_path.exists():
                        _, desc = extract_title_and_description(file_path)
                        description = desc

                    if description:
                        lines.append(f"- [{text}]({base_url}/{url_path}): {description}")
                    else:
                        lines.append(f"- [{text}]({base_url}/{url_path})")
                elif href and href.endswith(".md"):
                    url_path = href.replace(".md", ".html")
                    lines.append(f"- [{text}]({base_url}/{url_path})")

            lines.append("")
        elif "href" in item:
            # Top-level link
            text = item.get("text", "")
            href = item.get("href", "")

            if href and href.endswith(".qmd"):
                # This is a section header, treat it specially
                section_name = text
                url_path = href.replace(".qmd", ".html")

                file_path = docs_dir / href
                description = ""
                if file_path.exists():
                    _, desc = extract_title_and_description(file_path)
                    description = desc

                lines.append(f"## {section_name}")
                lines.append("")
                if description:
                    lines.append(f"- [{text}]({base_url}/{url_path}): {description}")
                else:
                    lines.append(f"- [{text}]({base_url}/{url_path})")
                lines.append("")

    content = "\n".join(lines)

    output_path = docs_dir / "llms.txt"
    output_path.write_text(content)
    print(f"Generated {output_path}")


if __name__ == "__main__":
    generate_llms_txt()

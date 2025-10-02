#!/usr/bin/env python3
"""Simple server to view Quarto docs as HTML."""

import http.server
import socketserver
import webbrowser
from pathlib import Path
import markdown
import sys

PORT = 8000


class QuartoHandler(http.server.SimpleHTTPRequestHandler):
    """Handler that converts .qmd to HTML on the fly."""

    def do_GET(self):
        """Handle GET requests."""
        # Convert path
        path = self.path.lstrip("/")
        if not path or path.endswith("/"):
            path += "index.qmd"

        file_path = Path(path)

        # Handle .qmd files
        if file_path.suffix == ".qmd" and file_path.exists():
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            # Read and convert to HTML
            content = file_path.read_text()

            # Simple YAML frontmatter extraction
            if content.startswith("---"):
                parts = content.split("---", 2)
                if len(parts) >= 3:
                    yaml_frontmatter = parts[1]
                    content = parts[2]

                    # Extract title
                    title = "Sidemantic Docs"
                    for line in yaml_frontmatter.split("\n"):
                        if line.startswith("title:"):
                            title = line.split(":", 1)[1].strip().strip('"')
                            break
                else:
                    title = "Sidemantic Docs"
            else:
                title = "Sidemantic Docs"

            # Convert markdown to HTML
            html = markdown.markdown(
                content,
                extensions=[
                    "fenced_code",
                    "codehilite",
                    "tables",
                    "toc",
                ],
            )

            # Wrap in HTML template
            full_html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            max-width: 900px;
            margin: 0 auto;
            padding: 2rem;
            color: #333;
        }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 0.5rem; }}
        h2 {{ color: #34495e; margin-top: 2rem; border-bottom: 1px solid #ecf0f1; padding-bottom: 0.3rem; }}
        h3 {{ color: #7f8c8d; }}
        code {{
            background: #f4f4f4;
            padding: 0.2rem 0.4rem;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }}
        pre {{
            background: #2c3e50;
            color: #ecf0f1;
            padding: 1rem;
            border-radius: 5px;
            overflow-x: auto;
        }}
        pre code {{
            background: transparent;
            color: inherit;
            padding: 0;
        }}
        a {{ color: #3498db; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1rem 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 0.75rem;
            text-align: left;
        }}
        th {{ background: #3498db; color: white; }}
        blockquote {{
            border-left: 4px solid #3498db;
            margin: 1rem 0;
            padding-left: 1rem;
            color: #7f8c8d;
        }}
        .nav {{
            background: #34495e;
            color: white;
            padding: 1rem;
            margin: -2rem -2rem 2rem -2rem;
        }}
        .nav a {{
            color: #3498db;
            margin-right: 1rem;
        }}
    </style>
</head>
<body>
    <div class="nav">
        <a href="index.qmd">Home</a>
        <a href="getting-started.qmd">Getting Started</a>
        <a href="concepts/models.qmd">Models</a>
        <a href="features/parameters.qmd">Parameters</a>
        <a href="features/symmetric-aggregates.qmd">Symmetric Aggregates</a>
        <a href="examples.qmd">Examples</a>
    </div>
    {html}
</body>
</html>
"""
            self.wfile.write(full_html.encode())
        else:
            # Fall back to default handler
            super().do_GET()


def main():
    """Start the server."""
    with socketserver.TCPServer(("", PORT), QuartoHandler) as httpd:
        print(f"\n{'=' * 60}")
        print(f"  Sidemantic Documentation Server")
        print(f"{'=' * 60}")
        print(f"\n  📚 Server running at: http://localhost:{PORT}")
        print(f"  🏠 View docs at: http://localhost:{PORT}/index.qmd")
        print(f"\n  Press Ctrl+C to stop")
        print(f"{'=' * 60}\n")

        # Open browser
        webbrowser.open(f"http://localhost:{PORT}/index.qmd")

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n\nServer stopped.")
            sys.exit(0)


if __name__ == "__main__":
    main()

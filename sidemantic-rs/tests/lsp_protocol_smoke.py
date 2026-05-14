import json
import queue
import subprocess
import sys
import threading
import time


def send_frame(proc, message):
    body = json.dumps(message, separators=(",", ":")).encode()
    proc.stdin.write(f"Content-Length: {len(body)}\r\n\r\n".encode())
    proc.stdin.write(body)
    proc.stdin.flush()


def read_frames(proc, messages):
    stream = proc.stdout
    while True:
        content_length = None
        while True:
            line = stream.readline()
            if not line:
                return
            if line in (b"\r\n", b"\n"):
                break
            header, _, value = line.decode().partition(":")
            if header.lower() == "content-length":
                content_length = int(value.strip())
        if content_length is None:
            continue
        body = stream.read(content_length)
        if not body:
            return
        messages.put(json.loads(body))


class LspClient:
    def __init__(self, binary):
        self.proc = subprocess.Popen(
            [binary],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.messages = queue.Queue()
        self.reader = threading.Thread(target=read_frames, args=(self.proc, self.messages), daemon=True)
        self.reader.start()

    def close(self):
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=5)

    def request(self, request_id, method, params):
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        send_frame(self.proc, message)
        return self.wait_for(lambda msg: msg.get("id") == request_id, f"id {request_id}")

    def notify(self, method, params):
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        send_frame(self.proc, message)

    def wait_for_notification(self, method):
        return self.wait_for(lambda msg: msg.get("method") == method, method)

    def wait_for(self, predicate, label):
        deadline = time.time() + 5
        seen = []
        while time.time() < deadline:
            try:
                msg = self.messages.get(timeout=0.25)
            except queue.Empty:
                if self.proc.poll() is not None:
                    stderr = self.proc.stderr.read().decode(errors="replace")
                    raise AssertionError(
                        f"LSP process exited while waiting for {label}; stderr={stderr!r}; seen={seen!r}"
                    )
                continue
            if predicate(msg):
                return msg
            seen.append(msg)
        raise AssertionError(f"timed out waiting for {label}; seen={seen!r}")


def completion_labels(response):
    return [item["label"] for item in response["result"]]


def hover_value(response):
    contents = response["result"]["contents"]
    assert contents["kind"] == "markdown", response
    return contents["value"]


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: lsp_protocol_smoke.py <sidemantic-lsp-binary>")
    client = LspClient(sys.argv[1])
    uri = "file:///tmp/sidemantic-lsp-smoke.semantic.sql"
    try:
        initialized = client.request(
            1,
            "initialize",
            {
                "processId": None,
                "rootUri": None,
                "capabilities": {"textDocument": {"publishDiagnostics": {}}},
            },
        )
        capabilities = initialized["result"]["capabilities"]
        assert capabilities["textDocumentSync"] == 1, capabilities
        assert isinstance(capabilities["completionProvider"], dict), capabilities
        assert capabilities["hoverProvider"] is True, capabilities
        assert capabilities["documentFormattingProvider"] is True, capabilities
        assert capabilities["documentSymbolProvider"] is True, capabilities
        assert capabilities["definitionProvider"] is True, capabilities
        assert capabilities["referencesProvider"] is True, capabilities
        assert capabilities["renameProvider"] is True, capabilities
        assert isinstance(capabilities["signatureHelpProvider"], dict), capabilities
        assert capabilities["codeActionProvider"] is True, capabilities
        client.notify("initialized", {})
        client.wait_for_notification("window/logMessage")

        client.notify(
            "textDocument/didOpen",
            {
                "textDocument": {
                    "uri": uri,
                    "languageId": "sidemantic",
                    "version": 1,
                    "text": "MODEL (name orders, table orders, primary_key order_id);",
                }
            },
        )
        diagnostics = client.wait_for_notification("textDocument/publishDiagnostics")
        assert diagnostics["params"]["uri"] == uri, diagnostics
        assert diagnostics["params"]["diagnostics"] == [], diagnostics

        keyword_hover = client.request(
            2,
            "textDocument/hover",
            {"textDocument": {"uri": uri}, "position": {"line": 0, "character": 2}},
        )
        assert "Top-level model definition" in hover_value(keyword_hover), keyword_hover

        property_hover = client.request(
            3,
            "textDocument/hover",
            {"textDocument": {"uri": uri}, "position": {"line": 0, "character": 9}},
        )
        assert "Unique model name" in hover_value(property_hover), property_hover

        symbols = client.request(
            9,
            "textDocument/documentSymbol",
            {"textDocument": {"uri": uri}},
        )
        assert [symbol["name"] for symbol in symbols["result"]] == ["orders"], symbols

        signature = client.request(
            10,
            "textDocument/signatureHelp",
            {"textDocument": {"uri": uri}, "position": {"line": 0, "character": 2}},
        )
        assert signature["result"]["signatures"][0]["label"].startswith("MODEL("), signature

        formatting = client.request(
            11,
            "textDocument/formatting",
            {"textDocument": {"uri": uri}, "options": {"tabSize": 4, "insertSpaces": True}},
        )
        assert formatting["result"], formatting
        assert "MODEL (\n    name orders," in formatting["result"][0]["newText"], formatting

        client.notify(
            "textDocument/didChange",
            {
                "textDocument": {"uri": uri, "version": 2},
                "contentChanges": [{"text": "MODEL ("}],
            },
        )
        diagnostics = client.wait_for_notification("textDocument/publishDiagnostics")
        diagnostic = diagnostics["params"]["diagnostics"][0]
        assert diagnostic["severity"] == 1, diagnostic
        assert diagnostic["source"] == "sidemantic-rs", diagnostic
        assert diagnostic["message"].startswith("Parse error:"), diagnostic

        client.notify(
            "textDocument/didChange",
            {
                "textDocument": {"uri": uri, "version": 3},
                "contentChanges": [{"text": "MODEL (name orders, table orders, primary_key order_id);\n\n"}],
            },
        )
        diagnostics = client.wait_for_notification("textDocument/publishDiagnostics")
        assert diagnostics["params"]["diagnostics"] == [], diagnostics

        rich_text = (
            "MODEL (name orders, table order_items, primary_key order_id);\n\n"
            "METRIC (name revenue, model orders, sql amount, agg sum);\n"
        )
        client.notify(
            "textDocument/didChange",
            {
                "textDocument": {"uri": uri, "version": 31},
                "contentChanges": [{"text": rich_text}],
            },
        )
        diagnostics = client.wait_for_notification("textDocument/publishDiagnostics")
        assert diagnostics["params"]["diagnostics"] == [], diagnostics

        definition = client.request(
            12,
            "textDocument/definition",
            {"textDocument": {"uri": uri}, "position": {"line": 2, "character": 28}},
        )
        assert definition["result"]["range"]["start"]["line"] == 0, definition

        refs = client.request(
            13,
            "textDocument/references",
            {
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": 12},
                "context": {"includeDeclaration": False},
            },
        )
        assert len(refs["result"]) == 1, refs
        assert refs["result"][0]["range"]["start"]["line"] == 2, refs

        rename = client.request(
            14,
            "textDocument/rename",
            {
                "textDocument": {"uri": uri},
                "position": {"line": 0, "character": 12},
                "newName": "sales_orders",
            },
        )
        edits = rename["result"]["changes"][uri]
        assert len(edits) == 2, rename
        assert all(edit["newText"] == "sales_orders" for edit in edits), rename

        client.notify(
            "textDocument/didChange",
            {
                "textDocument": {"uri": uri, "version": 32},
                "contentChanges": [{"text": "MODEL (name orders, table orders, primary_key order_id);\n\n"}],
            },
        )
        diagnostics = client.wait_for_notification("textDocument/publishDiagnostics")
        assert diagnostics["params"]["diagnostics"] == [], diagnostics

        top_level = client.request(
            4,
            "textDocument/completion",
            {"textDocument": {"uri": uri}, "position": {"line": 2, "character": 0}},
        )
        labels = completion_labels(top_level)
        for expected in ["MODEL", "DIMENSION", "METRIC", "RELATIONSHIP", "SEGMENT"]:
            assert expected in labels, labels

        client.notify(
            "textDocument/didChange",
            {
                "textDocument": {"uri": uri, "version": 4},
                "contentChanges": [{"text": "MODEL (\n    \n);"}],
            },
        )
        client.wait_for_notification("textDocument/publishDiagnostics")
        inside_model = client.request(
            5,
            "textDocument/completion",
            {"textDocument": {"uri": uri}, "position": {"line": 1, "character": 4}},
        )
        labels = completion_labels(inside_model)
        for expected in ["name", "table", "primary_key", "default_time_dimension", "sql"]:
            assert expected in labels, labels

        code_actions = client.request(
            15,
            "textDocument/codeAction",
            {
                "textDocument": {"uri": uri},
                "range": {"start": {"line": 0, "character": 0}, "end": {"line": 1, "character": 0}},
                "context": {
                    "diagnostics": [
                        {
                            "range": {
                                "start": {"line": 0, "character": 0},
                                "end": {"line": 0, "character": 10},
                            },
                            "message": "name field required",
                            "severity": 1,
                            "source": "sidemantic-rs",
                        }
                    ]
                },
            },
        )
        assert code_actions["result"][0]["title"] == "Add missing name property", code_actions

        unopened = client.request(
            6,
            "textDocument/completion",
            {
                "textDocument": {"uri": "file:///tmp/unopened.semantic.sql"},
                "position": {"line": 0, "character": 0},
            },
        )
        assert unopened["result"] == [], unopened

        yaml_uri = "file:///tmp/sidemantic-lsp-smoke.yml"
        client.notify(
            "textDocument/didOpen",
            {
                "textDocument": {
                    "uri": yaml_uri,
                    "languageId": "yaml",
                    "version": 1,
                    "text": "models:\n  - name: orders\n",
                }
            },
        )
        diagnostics = client.wait_for_notification("textDocument/publishDiagnostics")
        assert diagnostics["params"]["uri"] == yaml_uri, diagnostics
        diagnostic = diagnostics["params"]["diagnostics"][0]
        assert diagnostic["message"].startswith("Parse error:"), diagnostic

        invalid_method = client.request(7, "sidemantic/unsupported", {})
        assert invalid_method["error"]["code"] == -32601, invalid_method

        shutdown = client.request(8, "shutdown", None)
        assert "result" in shutdown, shutdown
        client.notify("exit", None)
        client.proc.stdin.close()
        deadline = time.time() + 5
        while time.time() < deadline and client.proc.poll() is None:
            time.sleep(0.05)
        assert client.proc.poll() is not None, "LSP process did not exit after shutdown"
    finally:
        client.close()


if __name__ == "__main__":
    main()

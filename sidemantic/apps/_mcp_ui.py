"""Vendored subset of mcp-ui-server (https://github.com/MCP-UI-Org/mcp-ui).

Provides create_ui_resource() for building MCP Apps-compatible UI resources.
Vendored because PyPI v1.0.0 has incorrect MIME type (text/html instead of
text/html;profile=mcp-app) and the fix hasn't been published yet.

License: Apache-2.0 (MCP UI Contributors)
"""

import base64
from typing import Any, Literal

from mcp.types import BlobResourceContents, EmbeddedResource, TextResourceContents
from pydantic import AnyUrl, BaseModel

# MIME type for MCP Apps resources
RESOURCE_MIME_TYPE = "text/html;profile=mcp-app"


class UIResource(EmbeddedResource):
    """A UI resource that can be included in tool results."""

    def __init__(self, resource: TextResourceContents | BlobResourceContents, **kwargs: Any):
        super().__init__(type="resource", resource=resource, **kwargs)


class RawHtmlPayload(BaseModel):
    type: Literal["rawHtml"]
    htmlString: str  # noqa: N815


class ExternalUrlPayload(BaseModel):
    type: Literal["externalUrl"]
    iframeUrl: str  # noqa: N815


class CreateUIResourceOptions(BaseModel):
    uri: str
    content: RawHtmlPayload | ExternalUrlPayload
    encoding: Literal["text", "blob"]


def create_ui_resource(options_dict: dict[str, Any]) -> UIResource:
    """Create a UIResource for inclusion in MCP tool results.

    Args:
        options_dict: Configuration with keys:
            - uri: Resource identifier starting with 'ui://'
            - content: {"type": "rawHtml", "htmlString": "..."} or {"type": "externalUrl", "iframeUrl": "..."}
            - encoding: "text" or "blob"

    Returns:
        UIResource (EmbeddedResource subclass) with correct MCP Apps MIME type.
    """
    options = CreateUIResourceOptions.model_validate(options_dict)

    if not options.uri.startswith("ui://"):
        raise ValueError(f"URI must start with 'ui://' but got: {options.uri}")

    content = options.content
    if isinstance(content, RawHtmlPayload):
        content_string = content.htmlString
    elif isinstance(content, ExternalUrlPayload):
        content_string = content.iframeUrl
    else:
        raise ValueError(f"Invalid content type: {content.type}")

    if options.encoding == "text":
        resource: TextResourceContents | BlobResourceContents = TextResourceContents(
            uri=AnyUrl(options.uri),
            mimeType=RESOURCE_MIME_TYPE,
            text=content_string,
        )
    elif options.encoding == "blob":
        resource = BlobResourceContents(
            uri=AnyUrl(options.uri),
            mimeType=RESOURCE_MIME_TYPE,
            blob=base64.b64encode(content_string.encode("utf-8")).decode("ascii"),
        )
    else:
        raise ValueError(f"Invalid encoding: {options.encoding}")

    return UIResource(resource=resource)

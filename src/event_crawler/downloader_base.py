import asyncio
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from typing import Annotated, ClassVar
from socket import timeout

from event_crawler.parser_base import ParserBase

CONTENT_TIMEOUT_S = 15

class DownloaderBase(ABC, ParserBase):
    """Abstract base class for calendar event downloaders."""

    url: Annotated[ClassVar[str],
        "URL of the downloader's target resource. Override in each subclass."]
    
    _registry: Annotated[ClassVar[dict[str, type["DownloaderBase"]]], # type: ignore
        "Registry of all downloader implementations. Automatically populated."] = {}

    class Error(ConnectionError):
        """Custom error type for download-related issues."""
        def __init__(self, is_recoverable: bool, *args: object) -> None:
            super().__init__(*args)
            self.is_recoverable = is_recoverable

    async def download(self) -> bytes:
        """Download the resource at the subclass's URL, and return its content as bytes."""
        def fetch() -> bytes:
            try:
                with urllib.request.urlopen(type(self).url, timeout=CONTENT_TIMEOUT_S) as response:
                    return response.read()
            except urllib.error.HTTPError as exc:
                raise DownloaderBase.Error(
                    exc.code in (408, 425, 429, 500, 502, 503, 504),
                    f"HTTP Error: {exc.code} - {exc.reason}"
                ) from exc
            except urllib.error.ContentTooShortError as exc:
                raise DownloaderBase.Error(
                    True, f"Content Too Short Error: {exc.reason}"
                ) from exc
            except urllib.error.URLError as exc:
                raise DownloaderBase.Error(
                    isinstance(exc.reason, timeout), f"URL Error: {exc.reason}"
                ) from exc
            except Exception as exc:
                raise DownloaderBase.Error(
                    False, f"Unexpected error during download: {exc}"
                ) from exc
        return await asyncio.to_thread(fetch)

    def decode_content(self, content: bytes) -> str:
        """Decode the downloaded content using the specified encoding."""
        return content.decode("utf-8")

    @abstractmethod
    async def extract_data(self, content: str) -> ParserBase.Result:
        """Extract event data from the downloaded content and return it as a list of dicts."""
        raise NotImplementedError
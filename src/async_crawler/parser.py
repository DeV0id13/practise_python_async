import asyncio
import logging
from typing import Any
from urllib.parse import urldefrag, urljoin, urlsplit
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class HTMLParser:

    async def parse_html(
            self,
            html: str,
            url: str,
    ) -> dict[str, Any]:
        """Преобразование HTML в структурированные данные."""
        return await asyncio.to_thread(self._parse_sync, html, url)

    def _parse_sync(
            self,
            html: str,
            url: str,
    ) -> dict[str, Any]:
        """Синхронный парсинг HTML."""
        soup = BeautifulSoup(html, "lxml")
        result = {
            "url": url,
            "title": "",
            "text": "",
            "links": [],
            "metadata": {"title": "", "description": "", "keywords": ""},
            "images": [],
            "headings": {"h1": [], "h2": [], "h3": []},
            "tables": [],
            "lists": [],
        }
        extractors = (
            ("metadata", self.extract_metadata, (soup,)),
            ("text", self.extract_text, (soup,)),
            ("links", self.extract_links, (soup, url)),
            ("images", self.extract_images, (soup, url)),
            ("headings", self.extract_headings, (soup,)),
            ("tables", self.extract_tables, (soup,)),
            ("lists", self.extract_lists, (soup,)),
        )

        for name, extractor, args in extractors:
            try:
                result[name] = extractor(*args)
            except Exception:
                logger.exception("Ошибка извлечения %s для %s", name, url)

        result["title"] = result["metadata"].get("title", "")
        return result

    def extract_links(
            self,
            soup: BeautifulSoup,
            base_url: str,
    ) -> list[str]:
        """Извлечение и нормализация ссылок."""
        links = []
        seen = set()

        for tag in soup.find_all("a", href=True):
            href = (tag.get("href") or "").strip()

            if not href or href.startswith("#"):
                continue

            try:
                url = urljoin(base_url, href)
                url, _ = urldefrag(url)
                parsed = urlsplit(url)
            except ValueError:
                continue

            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                continue

            if url not in seen:
                seen.add(url)
                links.append(url)

        return links

    def extract_text(
            self,
            soup: BeautifulSoup,
            selector: str | None = None,
    ) -> str:
        """Извлечение текста страницы или выбранного элемента."""
        if selector is not None:
            element = soup.select_one(selector)
        else:
            element = soup.body or soup
        if element is None:
            return ""
        return element.get_text(separator=" ", strip=True)

    def extract_metadata(
            self,
            soup: BeautifulSoup,
    ) -> dict[str, str]:
        """Извлечение title, description и keywords."""
        metadata = {
            "title": soup.title.get_text(strip=True) if soup.title else "",
            "description": "",
            "keywords": "",
        }

        for tag in soup.find_all("meta", attrs={"name": True}):
            name = (tag.get("name") or "").lower()

            if name in ("description", "keywords"):
                metadata[name] = (tag.get("content") or "").strip()

        return metadata

    def extract_images(
            self,
            soup: BeautifulSoup,
            base_url: str,
    ) -> list[dict[str, str]]:
        images = []

        for tag in soup.find_all("img", src=True):
            src = (tag.get("src") or "").strip()

            if not src:
                continue

            try:
                url = urljoin(base_url, src)
                parsed = urlsplit(url)

                if parsed.scheme not in ("http", "https") or not parsed.hostname:
                    continue

            except ValueError:
                continue

            images.append({
                "src": url,
                "alt": (tag.get("alt") or "").strip(),
            })

        return images

    def extract_headings(
            self,
            soup: BeautifulSoup,
    ) -> dict[str, list[str]]:
        """Извлечение h1, h2, h3."""
        headings = {}

        for level in ("h1", "h2", "h3"):
            headings[level] = []

            for tag in soup.find_all(level):
                text = tag.get_text(separator=" ", strip=True)
                headings[level].append(text)

        return headings

    def extract_tables(
            self,
            soup: BeautifulSoup,
    ) -> list[list[list[str]]]:
        """Извлечение таблиц."""
        tables = []

        for table in soup.find_all("table"):
            rows = []

            for row in table.find_all("tr"):
                # Вложенная таблица будет обработана отдельно
                if row.find_parent("table") is not table:
                    continue

                cells = []

                for cell in row.find_all(["th", "td"], recursive=False):
                    cells.append(cell.get_text(separator=" ", strip=True))

                rows.append(cells)

            tables.append(rows)

        return tables

    def extract_lists(
            self,
            soup: BeautifulSoup,
    ) -> list[dict[str, Any]]:
        """Извлечение ul и ol."""
        lists = []

        for list_tag in soup.find_all(["ul", "ol"]):
            items = []

            for item in list_tag.find_all("li", recursive=False):
                items.append(item.get_text(separator=" ", strip=True))

            lists.append({
                "type": list_tag.name,
                "items": items,
            })

        return lists

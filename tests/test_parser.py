import logging

import pytest
from aiointercept import aiointercept
from bs4 import BeautifulSoup

from async_crawler.crawler import AsyncCrawler
from async_crawler.parser import HTMLParser


HTML = """
<html><head><title>Example</title>
<meta name="description" content="Description"></head>
<body><h1>Heading</h1><a href="/page">Link</a>
<img src="/image.png" alt="Image">
<table><tr><td>Cell</td></tr></table><ul><li>Item</li></ul>
</body></html>
"""
URL = "https://example.com/"


@pytest.mark.parametrize(
    ("name", "default"),
    [
        ("metadata", {"title": "", "description": "", "keywords": ""}),
        ("text", ""),
        ("links", []),
        ("images", []),
        ("headings", {"h1": [], "h2": [], "h3": []}),
        ("tables", []),
        ("lists", []),
    ],
)
def test_partial_results(
    monkeypatch, caplog, name, default
):
    parser = HTMLParser()
    expected = parser._parse_sync(HTML, URL)

    def fail(*args):
        raise ValueError("extraction failed")

    monkeypatch.setattr(parser, f"extract_{name}", fail)
    with caplog.at_level(logging.ERROR):
        result = parser._parse_sync(HTML, URL)

    assert result[name] == default
    for key in expected.keys() - {name, "title"}:
        assert result[key] == expected[key]
    assert result["title"] == ("" if name == "metadata" else "Example")
    assert f"Ошибка извлечения {name}" in caplog.text


@pytest.mark.asyncio
async def test_parse_valid_html():
    result = await HTMLParser().parse_html(HTML, URL)

    assert result["url"] == URL
    assert result["title"] == "Example"
    assert result["metadata"] == {
        "title": "Example", "description": "Description", "keywords": ""
    }
    assert result["text"] == "Heading Link Cell Item"
    assert result["links"] == ["https://example.com/page"]
    assert result["images"] == [{"src": "https://example.com/image.png", "alt": "Image"}]
    assert result["headings"] == {"h1": ["Heading"], "h2": [], "h3": []}
    assert result["tables"] == [[["Cell"]]]
    assert result["lists"] == [{"type": "ul", "items": ["Item"]}]


@pytest.mark.asyncio
async def test_parse_broken_html():
    html = "<html><head><title>Broken</title></head><body><h1>Heading</h1><p>Text <b>bold"

    result = await HTMLParser().parse_html(html, URL)

    assert result["title"] == "Broken"
    assert result["headings"]["h1"] == ["Heading"]
    assert "Text bold" in result["text"]
    assert result["links"] == []


def test_extract_links():
    soup = BeautifulSoup("""
        <a href="../about#team">Relative</a>
        <a href="https://example.com/about">Duplicate</a>
        <a href="https://other.example/path#section">Absolute</a>
        <a href="#section">Anchor</a>
        <a href="mailto:user@example.com">Email</a>
        <a href="javascript:alert(1)">Script</a>
        <a href="https://[invalid">Malformed</a>
        <a href=" ">Empty</a><a>No href</a>
    """, "lxml")

    assert HTMLParser().extract_links(soup, "https://example.com/blog/") == [
        "https://example.com/about", "https://other.example/path"
    ]


def test_extract_images():
    soup = BeautifulSoup("""
        <img src="../cat.png" alt=" Cat ">
        <img src="https://cdn.example/logo.png">
        <img src="javascript:alert(1)" alt="Invalid">
        <img src="https://[invalid" alt="Malformed">
        <img src=" "><img alt="No src">
    """, "lxml")

    assert HTMLParser().extract_images(soup, "https://example.com/blog/") == [
        {"src": "https://example.com/cat.png", "alt": "Cat"},
        {"src": "https://cdn.example/logo.png", "alt": ""},
    ]


def test_extract_tables_and_lists():
    soup = BeautifulSoup("""
        <table><tr><th>Name</th><th>Value</th></tr>
        <tr><td>One</td><td><b>Two</b></td></tr></table>
        <ul><li>First</li><li>Second</li></ul>
        <ol><li>Start</li><li>Finish</li></ol>
    """, "lxml")
    parser = HTMLParser()

    assert parser.extract_tables(soup) == [
        [["Name", "Value"], ["One", "Two"]]
    ]
    assert parser.extract_lists(soup) == [
        {"type": "ul", "items": ["First", "Second"]},
        {"type": "ol", "items": ["Start", "Finish"]},
    ]


@pytest.mark.asyncio
async def test_fetch_and_parse():
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000)
    async with aiointercept() as mocked:
        url = f"{mocked.server_url}/page"
        mocked.get(url, status=200, body=HTML)
        try:
            result = await crawler.fetch_and_parse(url)
            assert result["url"] == url
            assert result["title"] == "Example"
            assert result["links"] == [f"{mocked.server_url}/page"]
            assert result["headings"]["h1"] == ["Heading"]
        finally:
            await crawler.close()

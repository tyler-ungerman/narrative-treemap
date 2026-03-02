from app.sources.rss_common import parse_rss_payload

RSS_SAMPLE_ONE = b"""
<rss version='2.0'>
  <channel>
    <title>Feed One</title>
    <item>
      <title>Alpha story</title>
      <link>https://example.com/alpha</link>
      <pubDate>Wed, 25 Feb 2026 12:00:00 GMT</pubDate>
      <description>Alpha summary</description>
    </item>
  </channel>
</rss>
"""

RSS_SAMPLE_TWO = b"""
<rss version='2.0'>
  <channel>
    <title>Feed Two</title>
    <item>
      <title>Beta report</title>
      <link>https://example.com/beta</link>
      <pubDate>Wed, 25 Feb 2026 11:00:00 GMT</pubDate>
      <description>Beta summary</description>
    </item>
  </channel>
</rss>
"""

RSS_SAMPLE_THREE = b"""
<rss version='2.0'>
  <channel>
    <title>Feed Three</title>
    <item>
      <title>Gamma update</title>
      <link>https://example.com/gamma</link>
      <pubDate>Wed, 25 Feb 2026 10:00:00 GMT</pubDate>
      <description>Gamma summary</description>
    </item>
  </channel>
</rss>
"""


def test_parse_rss_payload_three_feeds():
    first = parse_rss_payload(RSS_SAMPLE_ONE, max_items=10)
    second = parse_rss_payload(RSS_SAMPLE_TWO, max_items=10)
    third = parse_rss_payload(RSS_SAMPLE_THREE, max_items=10)

    assert first[0].title == "Alpha story"
    assert second[0].url == "https://example.com/beta"
    assert third[0].summary == "Gamma summary"

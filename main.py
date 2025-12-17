# main.py
from ad_chains_urls import AdChainsUrls
from ad_bids import AdBids
from ad_parameters import AdParameters
from ad_chain_content import AdChainContent
from ad_events import AdEvents
from ad_vast import AdVast
from ad_vast_parameters import AdVastParameters
from address_bar_url import AddressBarUrl
from navigation_screenshot import NavigationScreenshots
from page_tags import PageTags
from browser_url_sec import BrowserUrlsSeq
from session_id import SessionIdGen
from dom_content import DomContent


def main():
   AdChainsUrls().run()
   AdBids().run()
   AdParameters().run()
   AdChainContent().run()
   AdEvents().run()
   AdVast().run()
   AdVastParameters().run()
   AddressBarUrl().run()
   NavigationScreenshots().run()
   PageTags().run()
   BrowserUrlsSeq().run()
   SessionIdGen().run()
   DomContent().run()




if __name__ == "__main__":
    main()

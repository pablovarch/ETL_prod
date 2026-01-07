# main.py
from insert import ad_chains_urls
from insert import ad_bids
from insert import ad_parameters
from insert import ad_chain_content
from insert import ad_events
from insert import ad_vast
from insert import ad_vast_parameters
from insert import address_bar_url
from insert import navigation_screenshot
from insert import page_tags
from insert import browser_url_sec
from insert import session_id
from insert import dom_content
from update import domain_attributes
from update import domain_features
from update import subdomains
from insert import domain_discovery_features
from update import secondary_domains
from insert import secondary_domains_html
from update import domain_discovery
from insert import domain_discovery_html



def main():
   # insert scripts
   ad_chains_urls.AdChainsUrls().run()
   ad_bids.AdBids().run()
   ad_parameters.AdParameters().run()
   ad_chain_content.AdChainContent().run()
   ad_events.AdEvents().run()
   ad_vast.AdVast().run()
   ad_vast_parameters.AdVastParameters().run()
   address_bar_url.AddressBarUrl().run()
   navigation_screenshot.NavigationScreenshots().run()
   page_tags.PageTags().run()
   browser_url_sec.BrowserUrlsSeq().run()
   session_id.SessionIdGen().run()
   dom_content.DomContent().run()
   # update scripts
   domain_attributes.DomainAttributesSync().run()
   domain_features.DomainFeaturesSync().run()
   subdomains.SubdomainsSync().run()

   #secondary domains
   domain_discovery_features.DomainDiscoveryFeaturesSync().run()
   secondary_domains.SecondaryDomainsSync().run()
   secondary_domains_html.SecondaryDomainsHtmlSync().run()

   # domain discovery
   domain_discovery.DomainDiscoverySync().run()
   domain_discovery_html.DomainDiscoveryHtmlSync().run()

if __name__ == "__main__":
    main()

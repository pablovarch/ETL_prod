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
from insert import domain_attributes_from_prod
from insert import domain_discovery_from_prod
from insert import secondary_domains_from_prod
from insert import browser_profiles



def main():


    # -------- From Prod
    # domain_attributes_from_prod.DomainAttributesFromProd().run()
    # domain_discovery_from_prod.DomainDiscoveryFromProd().run()
    # secondary_domains_from_prod.SecondaryDomainsFromProd().run()
    # subdomains.SubdomainsSync().run()


    ## ------------ insert scripts
    session_id.SessionIdGen().run()
    browser_profiles.BrowserProfilesSync().run()
    ad_events.AdEvents().run()
    ad_chains_urls.AdChainsUrls().run()
    ad_chain_content.AdChainContent().run()
    ad_bids.AdBids().run()
    ad_vast.AdVast().run()
    ad_parameters.AdParameters().run()
    ad_vast_parameters.AdVastParameters().run()
    address_bar_url.AddressBarUrl().run()
    browser_url_sec.BrowserUrlsSeq().run()
    dom_content.DomContentSync().run()
    navigation_screenshot.NavigationScreenshots().run()
    page_tags.PageTags().run()



    ## ---------------- update scripts

    domain_attributes.DomainAttributesSync().run()
    # domain_features.DomainFeaturesSync().run()
    # subdomains.SubdomainsSync().run()

    # ------------------secondary domains
    secondary_domains.SecondaryDomainsSync().run()
    secondary_domains_html.SecondaryDomainsHtmlSync().run()
    domain_discovery_features.DomainDiscoveryFeaturesSync().run()

    # ---------------- domain discovery
    domain_discovery.DomainDiscoverySync().run()
    domain_discovery_html.DomainDiscoveryHtmlSync().run()



if __name__ == "__main__":
    main()

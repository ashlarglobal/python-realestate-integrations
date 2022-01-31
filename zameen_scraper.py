# packages
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import urllib
import json

class ZameenScraper(scrapy.Spider):
    name = 'zameen'
    
    # base URL
    base_url = 'https://www.zameen.com/Homes/'
    
    params = {
        'agent_id':'163133',
        'type':'all'
    }
    

    headers = {
     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'

    }
    
    # custom settings
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'zameen_home2.csv',
        
        # uncomment below to limit the spider speed
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        # 'DOWNLOAD_DELAY': 1
    }
    

    def start_requests(self):
        for page in range(1, 8):
            # generate next page URL
            next_page = self.base_url + 'Pakistan-1521-' + str(page) + '.html'
            next_page += urllib.parse.urlencode(self.params)
            
            # crawl the next page URL
            yield scrapy.Request(
                url=next_page,
                headers=self.headers,
                callback=self.parse
            )
    
    # parse property cards
    def parse(self, response):
        
        features = []
        for card in response.css('li[role="article"]'):
  
            feature = {
                'title': card.css('h2[aria-label="Title"]::text')
                             .get(),
                
                'price': 'PKR ' + card.css('span[aria-label="Price"]::text')
                             .get(),
                
                'location': card.css('div[aria-label="Location"]::text')
                                    .get(),
                
                'details_url': 'https://www.zameen.com' + card.css('a::attr(href)')
                                   .get(),
                
                'bedrooms': card.css('span[aria-label="Beds"]::text')
                                .get(),
                
                'bathrooms': card.css('span[aria-label="Baths"]::text')
                                .get(),
                                
                'area': card.css('span[aria-label="Area"] *::text')
                                .get(),
                
                'price': 'N/A',
                
                'purpose': 'N/A',
               
                'Property_type':'N/A',
                
                'phone': 'N/A',
                
                'contact_name': 'N/A',
                
                'img_url': 'N/A',

                'description':'N/a',
            }
            
            try:
                feature['img_url'] = card.css('source[type="image/webp"]::attr(data-srcset)').get().replace('400x300', '800x600')
            
            except:
                pass
            
            features.append(feature)
            
        try:
            json_data = ''.join([
                script.get() for script in
                response.css('script::text')
                if 'window.state = ' in script.get()
            ])
            
            json_data = json_data.split('window.state = ')[-1].split('}};')[0] + '}}'
            json_data = json.loads(json_data)
            json_data = json_data['algolia']['content']['hits']
            
            for index in range(0, len(features)):
                features[index]['price'] = json_data[index]['price'] 
                features[index]['purpose'] = json_data[index]['purpose']
                features[index]['property_type'] = json_data[index]['category'][-1]['name']
                features[index]['phone'] = ', '.join(json_data[index]['phoneNumber']['mobileNumbers'])
                features[index]['contact_name'] = json_data[index]['contactName']
                features[index]['description'] = json_data[index]['shortDescription']

                
              
                yield features[index]
        except:
            pass

if __name__ == '__main__':
    # run scraper
    process = CrawlerProcess()
    process.crawl(ZameenScraper)
    process.start()
    
    # debugging selectors
    # ZameenScraper.parse(ZameenScraper, '')
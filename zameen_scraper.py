# packages
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv
import mysql.connector
import scrapy
import urllib
import json
import os

load_dotenv()

class ZameenScraper(scrapy.Spider):
    name = 'zameen'

    # base URL
    base_url = 'https://www.zameen.com/Homes/'
    
    params = {
        
        'types':'all',
        'agent_id':'157272'
       
    }
    

    headers = {
     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'

    }

    
    def start_requests(self):
        for page in range(1, 9):
            # generate next page URL
            next_page = self.base_url + 'Pakistan-1521-' + str(page) + '.html?'
            next_page += urllib.parse.urlencode(self.params)
            
            # crawl the next page URL
            yield scrapy.Request(
                url=next_page,
                headers=self.headers,
                callback=self.parse
            )
    
    # parse property cards
    def parse(self, response):

        DB_HOST=os.getenv('DB_HOST')
        DB_PORT=os.getenv('DB_PORT')
        DB_DATABASE = os.getenv('DB_DATABASE')
        DB_USERNAME=os.getenv('DB_USERNAME')
        DB_PASSWORD=os.getenv('DB_PASSWORD')
    
        # Connect to database server
        cnx = mysql.connector.connect(
            host= DB_HOST,
            port= DB_PORT,
            user= DB_USERNAME,
            password= DB_PASSWORD,
            database  = DB_DATABASE)

        features = []
        for card in response.css('li[role="article"]'):
  
            feature = {
                #for properties table
                'title': card.css('h2[aria-label="Title"]::text')
                             .get(),

                'description':'N/a',

                # 'purpose': 'N/A',

                'area': card.css('span[aria-label="Area"] ::text')
                                .get(),
                
                'price': 'PKR ' + card.css('span[aria-label="Price"]::text')
                             .get(),
                
                # # for address table
                # 'location': card.css('div[aria-label="Location"]::text')
                #                     .get(),
                
              
                # # for property_details

                # 'rooms': card.css('span[aria-label="Beds"]::text')
                #                 .get(),
                
                # 'bathrooms': card.css('span[aria-label="Baths"]::text')
                #                 .get(),

                
                'price': 'N/A',
                
                # 'property_type':'N/A',
                
                
            }
            
            
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
                # features[index]['purpose'] = json_data[index]['purpose']
                # features[index]['property_type'] = json_data[index]['category'][-1]['name']
                features[index]['description'] = json_data[index]['shortDescription']

                
              
                yield features[index]
        except:
            pass

        for feature1 in features:
            columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in feature1.keys())
            values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in feature1.values())
            sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('property_drafts', columns, values)

          

        cursor = cnx.cursor()       
        cursor.execute(sql)
        cnx.commit()
        cursor.close()
        cnx.close()

if __name__ == '__main__':
    # run scraper
    process = CrawlerProcess()
    process.crawl(ZameenScraper)
    process.start()

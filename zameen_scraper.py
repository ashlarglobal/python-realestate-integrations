# packages
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv
import mysql.connector
import scrapy
import urllib
import json
import sys
import os

load_dotenv()

class ZameenScraper(scrapy.Spider):
    def __init__(self):
        self.create_conn()
        
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
    current_page = 1
    
    def parse_pagination(self, response):        
        
        try:  
            # extract number of total pages
            found_results = int(response.css('div[role="navigation"]').css('li *::text').get())
            total_pages = int(found_results / 1) + 1
            # increment curent page counter
            self.current_page += 1
        except:
            total_pages = 1        
        
        # loop over the range of pages
        for page in range(1, total_pages):
            # generate next page URL
            next_page = self.base_url + 'Pakistan-1521-' + str(page) + '.html'
            next_page += urllib.parse.urlencode(self.params)
            
         
            # crawl next page
            yield response.follow(
                url=next_page,
                headers=self.headers,
                callback=self.parse
            )
                # crawl the next page URL
            yield scrapy.Request(
                    url=next_page,
                    headers=self.headers,
                    callback=self.parse
            )


    def create_conn(self):
        DB_HOST=os.getenv('DB_HOST')
        DB_PORT=os.getenv('DB_PORT')
        DB_DATABASE = os.getenv('DB_DATABASE')
        DB_USERNAME=os.getenv('DB_USERNAME')
        DB_PASSWORD=os.getenv('DB_PASSWORD')
        # connect to Connect to DB
        try:
            self.cnx = mysql.connector.connect(
                                    user = DB_USERNAME,
                                    password = DB_PASSWORD,
                                    host = DB_HOST,
                                    port=DB_PORT,
                                    database = DB_DATABASE
                                    )
        except mysql.error as e:
            print(f"Error connecting to DB platform : {e}")
            sys.exit(1)

        self.curr = self.cnx.cursor()
        
  
    # parse property cards
    def parse(self, response):

        features = []
        for card in response.css('li[role="article"]'):
  
            feature = {
                #for properties table
                'title': card.css('h2[aria-label="Title"]::text')
                             .get(),

                'description':'N/a',

                'purpose': 'N/A',

                'area': card.css('span[aria-label="Area"] *::text')
                                .get(),
                
                'price': 'PKR ' + card.css('span[aria-label="Price"]::text')
                             .get(),
                
                # # for address table
                # 'location': card.css('div[aria-label="Location"]::text')
                #                     .get(),
                
              
                # # for property_details

                'rooms': card.css('span[aria-label="Beds"]::text')
                                .get(),
                
                'bathrooms': card.css('span[aria-label="Baths"]::text')
                                .get(),

                
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
            draft = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('property_drafts', columns, values)
            detail = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('property_details', columns, values)


        self.curr.execute(draft)
        self.curr.execute(detail)
        self.cnx.commit()
        self.curr.close()
        self.cnx.close()
      

if __name__ == '__main__':
    # run scraper
    process = CrawlerProcess()
    process.crawl(ZameenScraper)
    process.start()

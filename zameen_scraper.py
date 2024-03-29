# packages
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv
from datetime import datetime
import mysql.connector
import requests
import scrapy
import urllib
import json
import os

now = datetime.now()
formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

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

    def start_requests(self):
        page = 1
        # loop over the pages
        while True:
               # generate next page URL
            next_page = self.base_url + 'Pakistan-1521-' + str(page) + '.html?'
            next_page += urllib.parse.urlencode(self.params)
            page = page+1
            if requests.get(next_page).status_code == 404:
                break
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
        self.cnx = mysql.connector.connect(
                        user = DB_USERNAME,
                        password = DB_PASSWORD,
                        host = DB_HOST,
                        port=DB_PORT,
                        database = DB_DATABASE
                    )
        self.curr = self.cnx.cursor(buffered = True)     
    # parse property cards
    def parse(self, response):

        features = []
        for card in response.css('li[role="article"]'):

            feature = {

                'title': card.css('h2[aria-label="Title"]::text')
                             .get(),

                'description':'N/A',

                'purpose': 'N/A',

                'area': card.css('span[aria-label="Area"] ::text')
                            .get().replace(',','').replace(' sqft',''),
                
                'price': 'PKR ' + card.css('span[aria-label="Price"]::text')
                             .get(),

                'created_at': formatted_date,

                'updated_at': formatted_date,
                
                'location': card.css('div[aria-label="Location"]::text')
                                    .get(),

                'rooms': card.css('span[aria-label="Beds"]::text')
                                    .get(),
                    
                'bathrooms': card.css('span[aria-label="Baths"]::text')
                                    .get(),
 
                'price':'N/A',

                'property_type':'N/A'            
            }
            json_data = ''.join([
                script.get() for script in
                response.css('script::text')
                if 'window.state = ' in script.get()
            ])
            
            json_data = json_data.split('window.state = ')[-1].split('}};')[0] + '}}'
            json_data = json.loads(json_data)
            json_data = json_data['algolia']['content']['hits']

            for index in range(0,len(feature)):
                feature['price'] = json_data[index]['price'] 
                if json_data[index]['purpose'] == 'for-sale':
                     feature['purpose'] = 0
                else:
                     feature['purpose'] = 1
                feature['description'] = json_data[index]['shortDescription']
                feature['property_type'] = json_data[index]['category'][-1]['name']

                yield feature
            
            details = ("INSERT INTO property_details "
                            "(rooms, bathrooms, created_at, updated_at) "
                            "VALUES (%(rooms)s, %(bathrooms)s, %(created_at)s, %(updated_at)s)")

            self.curr.execute(details,feature)                  
            feature['property_detail_id'] = self.curr.lastrowid
            
            location = ("INSERT INTO addresses "
                                "(location, created_at, updated_at) "
                                "VALUES (%(location)s, %(created_at)s, %(updated_at)s)")

            self.curr.execute(location,feature)
            feature['address_id'] = self.curr.lastrowid

            category = ("INSERT INTO categories "
                                "(name, created_at, updated_at) "
                                "VALUES (%(property_type)s, %(created_at)s, %(updated_at)s)")

            self.curr.execute(category,feature)
            feature['category_id'] = self.curr.lastrowid

            drafts = ("INSERT INTO property_drafts "
                                "(title, description, area, price, purpose, category_id, property_detail_id, address_id, created_at, updated_at) "
                                "VALUES (%(title)s, %(description)s, %(area)s, %(price)s, %(purpose)s, %(category_id)s , %(property_detail_id)s,%(address_id)s, %(created_at)s, %(updated_at)s)")
                    
            self.curr.execute(drafts,feature)

            features.append(feature)
        
        self.cnx.commit()
        self.curr.close()
        self.cnx.close()        

if __name__ == '__main__':
    # run scraper
    process = CrawlerProcess()
    process.crawl(ZameenScraper)
    process.start()
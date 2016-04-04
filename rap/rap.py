import requests
from lxml import html
from time import sleep
from pymongo import MongoClient
from bs4 import BeautifulSoup


class scrape():


    def __init__(self):
        self.URL = 'http://lyrics.wikia.com/wiki/Category:Genre/Hip_Hop?'
        self.HEADERS   = {'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'}
        self.TIMEOUT   = 1
        self.ITER      = 1

    def get_links(self):
        client = MongoClient('localhost', 27017)
        db = client['RAP']

        for i in range(1,22):
        	sleep(.25)
	        result = requests.get(self.URL, headers = self.HEADERS)
	        bs_obj = BeautifulSoup(result.text, 'lxml')
	        AS     = bs_obj.find_all('a')
	        left   = filter(lambda x: x.text == 'next 200', AS)
	        left   = list (left)
	        if left != []:
	        	url = "http://lyrics.wikia.com"
	        	url += left[0].get('href')
	        	self.URL = url

	        	print (url)

		        db['PAGES'].insert(
		        {
		         "link": url
		         })
    def get_artists(self):
    	client = MongoClient('localhost', 27017)
    	db = client['RAP']
    	for url in db['PAGES'].find(): 
    		sleep(.26)
    		url = url['link']
    		result = requests.get(url, headers=self.HEADERS)
    		bs_obj = BeautifulSoup(result.text, 'lxml')
    		table     = bs_obj.select('#mw-pages > div > table')
    		AS = map (lambda x: x.select('a'), table)
    		AS = (list(AS)[0])
    		titles = map (lambda x: x.get("title"), AS)
    		hrefs  = map (lambda x: x.get("href"), AS)
    		hrefs_i  = map (lambda x: "http://lyrics.wikia.com" + x, hrefs)

    		for title, link in zip(titles, hrefs_i):
    			db["ARTISTS"].insert(
    				{
    				"title" : title,
    				"link"  : link
    				})
    			print (title, link)

    def get_lyrics(self):
    	client  = MongoClient('localhost', 27017)
    	db = client['RAP']
    	for artist in db['ARTISTS'].find():
    		url = artist['link']
    		sleep(1)
    		url = "http://lyrics.wikia.com/wiki/50_Cent"
    		result = requests.get(url)
    		bs_obj = BeautifulSoup(result.text, 'lxml')
    		#headers = bs_obj.select("h2 > span.mw-headline > a")
    		print(bs_obj.span.next_sibling)

    		break

if __name__ == '__main__':
    
    run = scrape()
    run.get_links()
    run.get_artists()

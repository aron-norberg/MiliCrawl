
import os
import time
import sys
import random

#import futures for concurrent threads
from concurrent import futures

# import urllib parse 
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from fake_useragent import UserAgent

import requests
from bs4 import BeautifulSoup
#import tqdm

DEFAULT_CONCUR_REQ = 30
MAX_CONCUR_REQ = 1000
MAX_WORKERS = 20

#Storage ::
from redis import Redis, RedisError

ua = UserAgent() # Generate a random user agent
proxies = [] # IP, Port

# Configure Local Settings for Redis
redis = Redis(host='localhost', port=6379, db=0, socket_connect_timeout=2, socket_timeout=2)

# Main Url Container
# key = url
# Values = (language, feature, response)

urlDict = {}
domain = "https://live-igcommerce.pantheonsite.io"

def random_proxy():

  return random.randint(0, len(proxies) - 1)

def retrieveProxies():
	proxies_req = Request('https://www.sslproxies.org/')
	proxies_req.add_header('User-Agent', ua.random)
	proxies_doc = urlopen(proxies_req).read().decode('utf8')

	soup = BeautifulSoup(proxies_doc, 'html.parser')
	proxies_table = soup.find(id='proxylisttable')

	for row in proxies_table.tbody.find_all('tr'):

		print(row.find_all('td')[0].string);

		proxies.append({
			'ip':   row.find_all('td')[0].string,
			'port': row.find_all('td')[1].string
		})

def populateURLList():

	urlList = redis.sscan("urls-from-solr-unknown", cursor=0, match="*", count=1000000)
	urlList = urlList[1]
	urlListSize = len(urlList)

	for i in range(len(urlList)):
		# Decode urls 
		url = urlList[i].decode('utf-8')

		# Obtain the language 
		language = url.split("#", 1)[1]
		language = language.split("#", 1)[0]
		language = language.split(':', 1)[-1]

		# Obtain the feature / If available 
		feature = url.split("#", 1)[1]
		feature = feature.split(':', 2)[-1]
		
		# Process Url -> remove #s
		url = url.split("#", 1)[0]

		response = 999
		#print(url)

		# Add items to Dictionary
		urlDict[url] = (language, feature, response)
		
	print(f"The size of the list is {urlListSize}.")

def printDictionary():

	for k, v in urlDict.items():
		print(k)
		print(f"language: {v[0]}")
		print(f"feature: {v[1]}")
		print(f"response: {v[2]}")

def getLanguage(href):

	parsedUrl = urlparse(href)
	language = parsedUrl.path.replace("/", "", 1)
	language = language.split("/", 1)[0]

	if len(language) < 2:
		return "en-us"
	else:
		if len(language) > 5: 
			return "en-us"
		else:
			return language

def getTemplate(content):

    soup = BeautifulSoup(content, 'html.parser')
    bodyNode = soup.find('body')

    for item in bodyNode['class']:
        if "template" in item:
            feature = item.replace("template-", "", 1)
            return feature

def addToRedisKnownList(resTuple):
	# Url:
	print(resTuple[0])

	# response Code:
	print(resTuple[1])

	# Feature Type:
	print(resTuple[2])

	# Language 
	print(resTuple[3])

def getRequest(url):

	#print(f'The url is {url}')

	#print(f'the domain + url is {domain}{url}')
	proxy_index = random_proxy()
	proxy = proxies[proxy_index]
	
	url = domain + url;

	try: 

		req = Request(url)

		# Possibly check using https
		req.set_proxy(proxy['ip'] + ':' + proxy['port'], 'http')
		#response = requests.get(url)

		language = getLanguage(url)

		#print(req.status_code);
		print("The request is: " + req);

		feature = getTemplate(req.content)

		return (url, req.status_code, feature, language)

	except Exception as error:
		print(f'The url is: {url}')
		print(f'The request error is {error}')

def getManyRequests(urlDict):

	count = 0

	with futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
		to_do = []

		print("scheduling Tasks..")

		for url in urlDict:
			parsedOutput = urlparse(url)
			url = parsedOutput.path

			future = executor.submit(getRequest, url)
			to_do.append(future)
			#msg = 'Scheduled for {}: {}'
			#print(msg.format(url, future))

		time.sleep(2)

		print("processing URLS...")

		results = []
		for future in futures.as_completed(to_do):
			res = future.result()
			msg = '{} result: {!r}'
			count += 1
			#print(msg.format(future, res))
			print(f"The count is {count}")
			# Add url to Redis
			addToRedisKnownList(res)
			results.append(res)

	return len(list(urlDict))

def main(getManyRequests):
	t0 = time.time()
	count = getManyRequests(urlDict)
	elapsed = time.time() - t0
	msg = '\n{} urls obtained in {:2f}s'
	print(msg.format(count, elapsed))

if __name__ == '__main__':

	retrieveProxies()
	populateURLList()
	main(getManyRequests)









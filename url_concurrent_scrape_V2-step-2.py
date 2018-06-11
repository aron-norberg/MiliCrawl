
import os
import ssl
import time
import sys
import random

#import futures for concurrent threads
from concurrent import futures

# import urllib parse 
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from fake_useragent import UserAgent

import requests
from bs4 import BeautifulSoup
#import tqdm

if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
    getattr(ssl, '_create_unverified_context', None)): 
    ssl._create_default_https_context = ssl._create_unverified_context

DEFAULT_CONCUR_REQ = 100
MAX_CONCUR_REQ = 100
MAX_WORKERS = 30

# Progress bar
import tqdm

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

	try:
		proxies_req = Request('https://www.sslproxies.org/')
		proxies_req.add_header('User-Agent', ua.random)
		proxies_doc = urlopen(proxies_req).read().decode('utf8')

		soup = BeautifulSoup(proxies_doc, 'html.parser')
		proxies_table = soup.find(id='proxylisttable')

		for row in proxies_table.tbody.find_all('tr'):

			#print(row.find_all('td')[0].string);

			proxies.append({
				'ip':   row.find_all('td')[0].string,
				'port': row.find_all('td')[1].string
			})

		print("All Proxies Have been obtained.")

	except:
		print ("Error Obtaining proxies. Job Exiting.")
		exit()

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

	#print("Adding to Known List. TODO - Actually add to the list.")
	#sys.stdout.flush()
	redis.sadd("urls-from-solr-all", f"{resTuple[0]}#language:{resTuple[3]}#feature:{resTuple[2]}")

def getRequest(url):

	success = False

	url = domain + url

	#url = url.replace(u'\xa0', u' ')

	url = url.encode('ascii', 'ignore').decode('ascii')
	#url = url.decode('utf-8')
	#print(url)
	language = getLanguage(url)
	#
	# TODO: Try a new proxy if one fails and delete the failed one from the list
	# - Consider using the same proxy for many requests in order to avoid randomizing everytime.
	# - Evaluate if set_proxy can use an https url, or if it works using http

	while success == False:

		proxy_index = random_proxy()
		proxy = proxies[proxy_index]
			
		try: 
				
			req = Request(url)

			req.add_header('User-Agent', ua.random)

			# Possibly check using https
			#req.set_proxy(proxy['ip'] + ':' + proxy['port'], 'https')

			response = urlopen(req)
			content = response.read()#.decode('utf8')
			status_code = response.getcode()
			feature = getTemplate(content)

			#if status_code != 200:
			#	feature = status_code

			#feature = "dummy";

			#print(f'The url is: {url}\n')
			#print(f"The status code is: {status_code}")
			#print(f"The feature is {feature}")
			#print(f"The language is {language}")

			success = True
			return (url, status_code, feature, language)
			
		except HTTPError as error:
			#success = True
			#print(f"error is {error}")
			feature = "404";
			status_code = "404";
			success = True

			return (url, status_code, feature, language)

		except URLError as error:
			print("Url error, try again with another proxy.\n")
			print(error.reason)
			#del proxies[proxy_index]
			success = False

		except UnicodeDecodeError as error:
			print(f'Unicode Error is {error} URL is: {url}\n')
			success = True

		except Exception as error:
			print(f'The Response Generic Error: {error} URl is: {url}\n')
			success = True


	return (url, status_code, feature, language)


def getManyRequests(urlDict):

	count = 0

	# ProcessPoolExecutor 
	# 
	with futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
		to_do_map = {}

		print("scheduling Tasks..")

		for url in urlDict:
			parsedOutput = urlparse(url)
			url = parsedOutput.path
			#print(url)

			future = executor.submit(getRequest, url)
			to_do_map[future] = url
			#msg = 'Scheduled for {}: {}'
			#print(msg)

		done_iter = futures.as_completed(to_do_map)

		print("processing URLS...")
			#print(msg.format(url, future))

		done_iter = tqdm.tqdm(done_iter, total=len(urlDict))

		#results = []
		for future in done_iter:
			try:
				res = future.result()
				#print(res)
				msg = '{} result: {!r}'
				count += 1
				#print(msg.format(future, res))
				#print(f"The count is {count}")
				# Add url to Redis
				addToRedisKnownList(res)
				#results.append(res)
			except Exception as error:
				print(f"Error at obtaining completed future. Error: {error}")

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









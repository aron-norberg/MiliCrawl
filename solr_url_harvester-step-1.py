import time
import urllib.request
import urllib.parse
from redis import Redis, RedisError
import json
from bs4 import BeautifulSoup
from progressbar import ProgressBar

# Configure Local Settings for Redis
redis = Redis(host='localhost', port=6379, db=0, socket_connect_timeout=2, socket_timeout=2)

# Data Structure to hold urls
urlDict = {}

# Assign the Domain to create urls
domain = "https://live-igcommerce.pantheonsite.io/"

langs = ["en-us" , "en-au" , "en" , "en-ca" , "en-gb" , "en-id" , "en-ie" , "en-in" , "en-my" , "en-ph" , "en-sg" , "en-th" , "en-vn" , "cs-cz" , "da-dk" , "de-de" , "de-at" , "de-ch" , "es-es" , "es-mx" , "es-ar" , "es-bo" , "es-cl" , "es-co" , "es-cr" , "es-do" , "es-ec" , "es-gt" , "es-pe" , "es-sv" , "es-us" , "es-uy" , "es-ve" , "fi-fi" , "fr-fr" , "fr" , "fr-be" , "fr-ca" , "fr-ch" , "id-id" , "it-it" , "ja-jp" , "ko-kr" , "nl-nl" , "nl-be" , "no-no" , "pl-pl" , "pt-br" , "pt-pt" , "ru-ru" , "sv-se" , "th-th" , "tr-tr" , "vi-vn" , "cn" , "zh-tw"]

#langs = ["en-us"]

# Authentication parameters
username = "flukesolr"
password = "V5GceLWmXywi729gH"

top_level_url = "http://fluke2xl-master.solrcluster.com"

# Create Password Manager
password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
password_mgr.add_password(None, top_level_url, username, password)

# Create Handler
handler = urllib.request.HTTPBasicAuthHandler(password_mgr)

# create "opener" (OpenerDirector instance)
opener = urllib.request.build_opener(handler)

# now use to open urls:
#opener.open(url)
def storeUnkownUrlsToRedis():
	pass

def getFeatureType(type, label):

	## F3 URLS
	if type == "toc" and "FNP" not in label and "Promotions and contests" not in label:
		return "f3"

	## F8 URLS
	elif type == "toc" and "Promotions and contests" in label:
		return "f8"

		## F13 URLS
	elif type == "toc" and "FNP" in label:
		return "f13"

	## F4 URLS
	elif type == "product_display":
		return "f4"

	## F14 URLS 
	##
	## NEEDS VERIFICACTION
	##
	elif type == "article":
		return "f14-tentative"

	##
	## <-- TOCs processed here -->
	##

	# F23 - Where to buy 
	elif type == "page":
		return "f23"

	else:
		return "unknown"


def addUrlToDict(url, language, feature):
	urlDict[url] = (language, feature)


def addf1Urls():
	for language in langs:

		## remove Domain From url creation

		url = domain + language
		urlDict[url] = (language, "f1")

def addDictToRedis():

	count = 0 
	f14Count = 0
	for k, v in urlDict.items():
		#print(k)
		#print(f"language: {v[0]}")
		#print(f"feature: {v[1]}")
		if v[1] == "unknown":
			redis.sadd("urls-from-solr-unknown", f"{k}#language:{v[0]}#feature:{v[1]}")
			count += 1

		elif v[1] == "f14-tentative":
			#redis.sadd("urls-from-solr-f14", f"{k}#language:{v[0]}#feature:{v[1]}")
			redis.sadd("urls-from-solr-unknown", f"{k}#language:{v[0]}#feature:{v[1]}")
			count += 1
		else:
			redis.sadd("urls-from-solr-all", f"{k}#language:{v[0]}#feature:{v[1]}")

	print(f"The dictionary count is -----------> {len(urlDict)}")
	print(f"Total Number of urls to crawl is --> {count}")
	#print(f"Urls with known features are  -----> {len(urlDict)-count}")
	#print(f"f14 verification requires ---------> {f14Count}")
	print(f"All Unknown Urls added to reddis set -> urls-from-solr-unkown")

def main():

	# Add F1's to dictionary
	addf1Urls()

	# Initialize a simple counter

	
	counter = 0
	featureType = ""

	# Assign types for solr query
	types = ["product_display" , "article" , "toc", "page", "url_builder"]

	# runs the progress bar
	pbar = ProgressBar()

	print("---------------------------")
	print("Starting SOLR Queries . . .")
	print("---------------------------")

	for lang in pbar(langs):
		if lang == "":
			break

		for type in types:
			url = f'http://fluke2xl-master.solrcluster.com/solr/fluke2xl/select?fq=bundle:"{type}"&fq=ss_language:"{lang}"&wt=json&indent=true&start=0&rows=1200&fl=path_alias,ss_path_alias,label'
			success = False

			while success == False:
				try:

					response = opener.open(url) # opens the SOLR page
					content = response.read() # reads SOLR page
					json_data = json.loads(content.decode('utf-8')) # loads the JSON content and decodes JSON lang
					success = True

					try:
						count = json_data['response']['numFound']
						count = int(BeautifulSoup(str(count), 'html.parser').get_text())

						for i in range(count):
							skip = False

							if type in ["url_builder", "toc"]:
								KeyPath = "ss_path_alias"

							else:

								KeyPath = "path_alias"

							try:

								## obtain the Path from the node 	
								path = json_data['response']['docs'][i][KeyPath]
								path = BeautifulSoup(str(path), 'html.parser').get_text()

								label = json_data['response']['docs'][i]['label']
								label = BeautifulSoup(str(label), 'html.parser').get_text() 

								featureType = getFeatureType(type, label)

							except KeyError:
								skip = True

							except IndexError:
								skip = True

							##### end of alias try

							if skip == False:

								results = domain + lang + "/" + path  #rebuild the full URL from the alias and lang

								#print(f"{results}");

								addUrlToDict(results, lang, featureType)
									
									#outfile.write(results + "\n")

					##### end of count loop
					except KeyError:
						print("No numFound")
					except IndexError:
						print("Index Error")

						##### end of numFound try
					##### end of types loop
				except: 
					success = False

	##### end of lang loop
	print("---------------------------")
	print("Ending SOLR Queries . . . .")
	print("---------------------------")
	#print ("The final count is %d" % counter)


if __name__ == '__main__':

	main()

	#number = urlListSize

    #loop = asyncio.get_event_loop()
    #future = asyncio.ensure_future(run(number))
    #loop.run_until_complete(future)

	addDictToRedis()






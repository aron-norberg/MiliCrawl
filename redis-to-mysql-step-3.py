
# Import redis data to mysql data
import mysql.connector
from redis import Redis, RedisError
from mysql.connector import errorcode
from urllib.parse import urlparse

# Configure Mac Settings for Redis
redis = Redis(host='localhost', port=6379, db=0, socket_connect_timeout=2, socket_timeout=2)

urlDict = {}

## #####################
##
## DATABASE CONNECTION 
##
## #####################

## MYSQL Configuration Data:
config = {
  'user': 'root',
  'password': 'root',
  'host': '127.0.0.1',
  'database': 'test',
}

try:
  connection = mysql.connector.connect(**config)
  cursor = connection.cursor()

except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
  print ("connection Successful.")


## #####################
##
## Truncate Url Table
## Purpose: SQL Query to clean url table 
## Before insterting new urls
## 
## #####################

def truncateUrlTable():

 	print("Truncating Urls Table...")
 	cursor.execute("truncate Urls")	
 	connection.commit()

#######################
##
## Populate Url Database List
## Purpose: Obtain Urls from redis Database 
## and populate MySQL database
## 
## #####################


def populateURLList():

	urlList = redis.sscan("urls-from-solr-all", cursor=0, match="*", count=1000000)
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

#######################
##
## Read the Url dictionary
## Purpose: Loop through the dictionary
## 
## Does not add if feature is None 
## or if feature is a status Code
## 
## #####################

def addDictToDB():

	count = 0
	antiCount = 0
	for k, v in urlDict.items():
		url = urlparse(k)
		if "F" in v[1] or "f" in v[1] and len(v[1]) < 5:
			feature = v[1].upper()
			count += 1
			print(url.path[1:]) # url
			print(f"language: {v[0]}")
			print(f"feature: {feature}")
			print(f"response: {v[2]}")
			print(f"The count is {count}")
			addUrlToDB(url.path[1:], feature, v[0])
		else:
			antiCount += 1
			print(url.path[1:]) # url
			print(f"language: {v[0]}")
			print(f"feature: {feature}")
			print(f"response: {v[2]}")
			print(f"The anti-count is {antiCount}")


#######################
##
## Add url Dictionary to Mysql db from Dictionary
## Purpose: Write the URl to the database
## 
## #####################

def addUrlToDB(url, language, feature):

  add_url = ("INSERT INTO Urls"
               "(Url, TemplateId, Language) "
               "VALUES (%s, %s, %s)")

  data_url = (url, language, feature)
  cursor.execute(add_url, data_url)
  connection.commit()


#######################
##
## Add url Dictionary to Mysql db from Dictionary
## Purpose: Write the URl to the database
## 
## #####################


def main():
	pass

if __name__ == '__main__':
	main()

	truncateUrlTable()
	populateURLList()
	addDictToDB()
	print("process complete. Urls have been added to the database.")
	print("Happy Testing. :)")

	
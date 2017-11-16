#!/usr/bin/env python3
#Author Sergio Gonzalez
import configparser
import sys
from urllib.request import urlopen
import hashlib, re, json
import syslog
from bs4 import BeautifulSoup
from modules import WebCrawler

from threading import Thread

docRoot='/home/sergio/techObservatory/'
countryConfigFile=docRoot+'etc/countries.ini'
techConfigFile='https://segonza.blob.core.windows.net/statics/technologies.txt'

syslog.openlog(logoption=syslog.LOG_PID)
debug=False

class Downloader(Thread):

	def __init__(self, country, tech):
		self.country = country
		self.tech = tech
		Thread.__init__(self, name=tech+'.'+country)
		
	def run(self):
		self.logMessage('INIT: CC={} Technology={}'.format(self.country, self.tech))
			
		countryConfig = configparser.ConfigParser()
		countryConfig.read(countryConfigFile)
		#### CHECK CC CONFIG
		if self.country not in countryConfig :
			self.logMessage('ERROR: No CC {} in config'.format(self.country))
			self.logMessage('EXIT: Error')
			exit()
		else:
			self.url=countryConfig [country]['url']
			self.indexOfOfferCountInResult=int(countryConfig [country]['index'])
			self.debugMessage('INIT: CC={} Index={} URL={}'.format(self.country,self.indexOfOfferCountInResult,self.url))

		self.webCrawler=WebCrawler.WebCrawler()
		
		self.downloadTech(self.country, self.tech)
	
	#### LOG HANDLING ####
	def logMessage(self, message):
		if debug:
			print (message)
		syslog.syslog(message)
	def debugMessage(self, message):
		if debug:
			syslog.syslog("DEBUG: "+message)	
		
	#### API URL HANDLING ####
	def callAPI(self, url):
		soap = BeautifulSoup(self.webCrawler.callAPI(url), 'lxml')
		return soap
	def buildBasicURL(self, topic):
		parameterizedURL='{}&q={}'.format(self.url, topic)
		return parameterizedURL	
	def buildPageURL(self, topic, pageNum):
		parameterizedURL='{}&q={}&start={}'.format(self.url, topic, pageNum)
		return parameterizedURL

	#### API CONTENT HANDLING ####
	def countTotalOffers(self, url):
		soup=self.callAPI(url)
		counterResult=soup.find('div', { 'id' : 'searchCount' })
		if counterResult is None:
			return 0
		else:
			counter=counterResult.text
			offerCount=counter.split()[self.indexOfOfferCountInResult].replace('.','')

			return offerCount	
	def countPages(self, url):
		offerCount=self.countTotalOffers(url)
		numPages=int(offerCount) - int(offerCount)%10

		return numPages	
	def getOffers(self, url):	
		jsonOffersArray = []
		self.debugMessage("Starting download {}".format(url))
		soup=self.callAPI(url) 
		self.debugMessage("Finished download {}".format(url))
		script = soup.find('script', text=re.compile('jobmap\['))
		
		if script is None:
			return None
		
		rawOffers=re.findall(r'jobmap\[[0-9]\]= {.+};', script.contents[0]	)
		
		for rawOffer in rawOffers:
			rawOffer=re.search(r'jobmap\[[0-9]\]= {(.+)};', rawOffer, flags=re.DOTALL | re.MULTILINE).group(1)
			rawOffer=rawOffer.replace("jk:'"        , "'jk':'")	
			rawOffer=rawOffer.replace("',efccid:"   , "','efccid':")
			rawOffer=rawOffer.replace("',srcid:'"   , "','srcid':'")			
			rawOffer=rawOffer.replace("',cmpid:'"   , "','cmpid':'")
			rawOffer=rawOffer.replace("',num:'"     , "','num':'")	
			rawOffer=rawOffer.replace("',srcname:'" , "','srcname':'")			
			rawOffer=rawOffer.replace("',cmp:'"     , "','cmp':'")				
			rawOffer=rawOffer.replace("',cmpesc:'"  , "','cmpesc':'")				
			rawOffer=rawOffer.replace("',cmplnk:'"  , "','cmplnk':'")				
			rawOffer=rawOffer.replace("',loc:'"     , "','loc':'")
			rawOffer=rawOffer.replace("',country:'" , "','country':'")
			rawOffer=rawOffer.replace("',zip:'"     , "','zip':'")
			rawOffer=rawOffer.replace("',city:'"    , "','city':'")
			rawOffer=rawOffer.replace("',title:'"   , "','title':'")		
			rawOffer=rawOffer.replace("',locid:'"   , "','locid':'")
			rawOffer=rawOffer.replace("',rd:'"      , "','rd':'")	
			
			rawOffer="{"+rawOffer+"}"
			rawOffer=rawOffer.replace("'", '"')					
							
			jsonOffer=json.loads(rawOffer)		
			jsonOffersArray.append(jsonOffer)
		self.debugMessage("Offer clean-up done {}".format(url))
		return jsonOffersArray
			
	#### OFFER MASSAGING ####
	def getOfferID(self, offer):
		return offer["jk"]
	def getCompanyID(self, offer):
		companyID=offer["cmpid"]
		return offer["cmpid"]	
	def getCompanyName(self, offer):
		companyName=offer["cmpesc"]
		if (companyName == "" ):
			companyName=offer["srcname"]
		return companyName	
	def getOfferData(self, offers, tech):
		self.debugMessage('Starting offer procesing')
		
		if offers is None:
			return None
		
		for offer in offers:
			offerID=self.getOfferID(offer)
			companyID=self.getCompanyID(offer)
			companyName=self.getCompanyName(offer)
			self.debugMessage('Read data from offer')
			self.webCrawler.checkCompanyInDB(companyID, companyName)
			self.debugMessage('Company DB check from offer')
			self.webCrawler.checkOfferInDB(offerID, companyID, country, tech)	
			self.debugMessage('Offer DB check from offer')
		self.debugMessage('Offers processed')

	#### CORE ####
	def downloadTech(self, country, tech):
		self.debugMessage('Start reading CC={} Technology={}'.format(country, tech))

		basicURL=self.buildBasicURL(tech)
		#Get the number of pages to interate
		numPages=self.countPages(basicURL)
		numOffers=self.countTotalOffers(basicURL)

		self.logMessage('RUN: CC={} Offers={} Technology={}'.format(country, numOffers, tech))      
		if numPages > 1:
			self.getOfferData(self.getOffers(basicURL), tech)
			for currentPageNum in range(10, numPages +10 , 10 ):
				thisURL=self.buildPageURL(tech, currentPageNum)
				self.getOfferData(self.getOffers(thisURL), tech)
		else:
			self.getOfferData(self.getOffers(basicURL), tech)

		self.debugMessage('DONE: CC={} Offers={} Technology={}'.format(country, numOffers, tech))      
				
		self.logMessage('END: CC={} Technology={}'.format(country, tech))	
	

#### CHECK INPUT ARGUMENTS
if len(sys.argv) < 2:
	print('ERROR: No CC suplied')
	exit()
else:
	country=sys.argv[1]

#### CORE ALGORITHM ####
techFileData  = urlopen(techConfigFile).read()
techFileLines = techFileData .decode('utf-8').split('\n')
try:
	for techology in techFileLines:
		Downloader(country, techology).start()
		
except Exception as inst:
	print('ERROR '+str(inst))

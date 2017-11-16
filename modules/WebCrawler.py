#!/usr/bin/env python3
#Author Sergio Gonzalez

import requests
import json
import datetime

from modules import DBDriver

class WebCrawler:

	def __init__(self):
		self.dbConnector=DBDriver.MSSQLDriver()

	def callAPI(self, urlAPI, **keywords):
		if "headers" in keywords:
			resp = requests.get( urlAPI, headers=keywords["headers"])		
		else:
			resp = requests.get( urlAPI)
				
		return resp.text

	def checkCompanyInDB(self, companyID, companyName):
		companyName=self.cleanCompanyName(companyName)
		self.dbConnector.checkCompany(companyID, companyName)
	
	def checkOfferInDB(self, offerID, companyID, country, cloud):
		self.dbConnector.checkOffer(offerID, companyID, country, cloud)
	
	def printCompanyData(self, companyID, companyName):
		print ('CompanyID: {}  Name: {}'.format(companyID, companyName))

	def printOfferData(self, offerID, companyID, country, cloud):
		print ('OfferID: {} CompanyID: {} Country: {} Cloud: {}'.format(offerID, companyID, country, cloud))
	
	def cleanCompanyName(self, companyName):
		companyName=companyName.replace("*" , "")
		companyName=companyName.replace("-" , "")
		companyName=companyName.replace("&amp;" , "&")
		companyName=companyName.replace("O&#039;" , "&")
		return companyName
	
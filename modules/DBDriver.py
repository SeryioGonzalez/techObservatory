#!/usr/bin/env python3
#Author Sergio Gonzalez

#import MySQLdb
import pymssql
import configparser

class MSSQLDriver:

	docRoot='/home/sergio/techObservatory/'
	databaseConfigFile=docRoot+'etc/dbaccess.ini'
	
	databaseConfig = configparser.ConfigParser()
	databaseConfig.read(databaseConfigFile)
	
	server   = databaseConfig ['dbinfo']['server']
	username = databaseConfig ['dbinfo']['username']
	password = databaseConfig ['dbinfo']['password']
	database = databaseConfig ['dbinfo']['database']

	def __init__(self):
		self.initConnection()

	def initConnection(self):
		self.dbConnection = pymssql.connect(server=self.server,
							 port=1433,
							 user=self.username,
							 password=self.password,
							 database=self.database,
							 tds_version='7.2')      

		self.dbCursor=self.dbConnection.cursor()

	def checkCompany(self, companyID, companyName):
		id=companyID
		name=companyName[0:50].replace("–"  , "-")		
		checkQuery="SELECT COUNT(*) FROM companies WHERE companyID='{}'".format(id)
		
		self.executeSelectQuery(checkQuery)

		(rows,)=self.dbCursor.fetchone()
		if rows == 0:
			insertQuery="INSERT INTO companies (companyID, companyName, firstSeen) VALUES ('{}', '{}', GETDATE())".format(id, name)	
			self.executeInsertQuery(insertQuery)	

	
	def checkOffer(self, offerID, companyID, country, tech):
		query="BEGIN TRAN IF EXISTS(SELECT * FROM offers WHERE offerID = '{}' AND tech = '{}') BEGIN UPDATE offers SET lastSeen = GETDATE() WHERE offerID = '{}' AND tech = '{}' END ELSE BEGIN INSERT INTO offers (offerID, tech, companyID, country, firstSeen, lastSeen) VALUES ('{}', '{}', '{}', '{}', GETDATE(), GETDATE()) END COMMIT TRAN".format(offerID, tech, offerID, tech, offerID, tech, companyID, country, tech)
		self.executeInsertQuery(query)

	def executeSelectQuery(self, query):
		self.dbCursor.execute(query)
		return

	def executeInsertQuery(self, query):
		self.dbCursor.execute(query)
		self.dbConnection.commit()
		return	
		
	def executeReadQuery(self, query):
		self.dbCursor.execute(query)
		self.dbConnection.commit()
		return self.dbCursor.fetchone()
		
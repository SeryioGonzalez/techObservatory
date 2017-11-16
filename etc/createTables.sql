

CREATE TABLE companies (
	companyID   nvarchar(50) NOT NULL, 
	companyName nvarchar(50),
	firstSeen DATETIME,
	PRIMARY KEY (companyID)
);

CREATE TABLE offers(
	offerID   varchar(50) NOT NULL, 
	tech      varchar(50) NOT NULL,
	companyID nvarchar(50), 
	country   varchar(2),
	firstSeen DATETIME,
	lastSeen  DATETIME,

	PRIMARY KEY (offerID, tech),
	FOREIGN KEY (companyID) 
	REFERENCES companies(companyID)
);

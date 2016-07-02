#Name: Abhinav Joshi
#Course Number: CSE 6331 Section 004
#Lab Number: 2 Part 2
'''Copyright (c) 2015 HG,DL,UTA
   Python program runs on Google AppEngine '''

import cgi
import webapp2
from google.appengine.ext.webapp.util import run_wsgi_app

import MySQLdb
import os
import jinja2
import csv
import cloudstorage as gcs


default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                      max_delay=5.0,
                                      backoff_factor=2,
                                      max_retry_period=15)
gcs.set_default_retry_params(default_retry_params)

# Configure the Jinja2 environment.
JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

# Define your production Cloud SQL instance information.
_INSTANCE_NAME = 'assignment1-973:earthquakedata'
_BUCKET_NAME = 'assignmentcloud1'
_CSV_FILE = 'all_month.csv'
#https://cloud.google.com/appengine/docs/python/gettingstartedpython27/usingdatastore refered this code        
class MainPage(webapp2.RequestHandler):
    def get(self):

         # Display existing Earthquake entries and a form to add new entries.
        if (os.getenv('SERVER_SOFTWARE') and
            os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
            db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='Earthquake', user='root', passwd='root')
        else:
            # db = MySQLdb.connect(host='127.0.0.1', port=3306, db='Earthquake', user='root', charset='utf 8')
            # Alternatively, connect to a Google Cloud SQL instance using:
            db = MySQLdb.connect(host='173.194.225.166', db='Earthquake', port=3306, user='root')

        # Create table and populate 
        self.createTable(db)
        self.truncateData(db)
        self.populateData(db)
        
        
        # Get weekly earth quake data for predefined values
        magnitudeTwo = self.getData(db, '2', 1)
        magnitudeThree = self.getData(db, '3', 1)
        magnitudeFour = self.getData(db, '4', 1)
        magnitudeFive = self.getData(db, '5', 1)
        magnitudeGreaterFive = self.getData(db, 'gt5', 2)
    
        # After usage closing the connection to DB
        db.close()
        
        variables = {'magnitudeTwo': magnitudeTwo,
                     'magnitudeThree': magnitudeThree,
                     'magnitudeFour': magnitudeFour,
                     'magnitudeFive': magnitudeFive,
                     'magnitudeGreaterFive': magnitudeGreaterFive}
        template = JINJA_ENVIRONMENT.get_template('main.html')
        self.response.write(template.render(variables))

    def getData(self, db, magnitude, query_type):
        cursor = db.cursor()
        if query_type == 1:
            cursor.execute('SELECT WEEK(time) WEEK, COUNT(1) NO FROM \
                            Earthquake.MagnitudeData WHERE mag<=%s and mag>%s GROUP BY WEEK(time)', (magnitude, int(magnitude)-1))
        else:
            cursor.execute('SELECT WEEK(time) WEEK, COUNT(1) NO FROM Earthquake.MagnitudeData WHERE mag>%s GROUP BY WEEK(time)', ('5'))

        weeklyData = [];
        for row in cursor.fetchall():
            weeklyData.append(dict([('week',row[0]),
                                 ('count',row[1])
                                 ]))
        return weeklyData
        

    def createTable(self, db):
        createQuery = 'CREATE TABLE IF NOT EXISTS Earthquake.MagnitudeData(\
                        time DATETIME NOT NULL,\
                        latitude FLOAT NOT NULL,\
                        longitude FLOAT NOT NULL,\
                        depth FLOAT NOT NULL,\
                        mag FLOAT NOT NULL,\
                        magType VARCHAR(25),\
                        nst INT,\
                        gap INT,\
                        dmin FLOAT,\
                        rms FLOAT NOT NULL,\
                        net VARCHAR(5) NOT NULL,\
                        id VARCHAR(25) NOT NULL,\
                        updated DATETIME NOT NULL,\
                        place VARCHAR(255) NOT NULL,\
                        type VARCHAR(25) NOT NULL,\
                        PRIMARY KEY(id))'
        cursor = db.cursor()
        cursor.execute(createQuery)
        db.commit()

    def truncateData(self, db):
        cursor = db.cursor()
        truncateQuery = '''TRUNCATE Earthquake.MagnitudeData''';
        cursor.execute(truncateQuery)
        db.commit()
        
    def populateData(self, db):
        bucket = '/' + _BUCKET_NAME
        fileName = bucket + '/' + _CSV_FILE
		#https://docs.python.org/2/library/csv.html This code is referred from this website, as a reference for reading the csv file
        gcsFile = gcs.open(fileName)
        reader = csv.DictReader(gcsFile)
        insertQuery = 'INSERT INTO Earthquake.MagnitudeData(\
                        time, latitude, longitude, depth, mag, magType,\
                        nst, gap, dmin, rms, net, id, updated, place, type)\
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s)'
        counter = 0
        cursor = db.cursor()
        entriesList = []
        for row in reader:
            entriesList.append((row['time'], row['latitude'], row['longitude'],
                           row['depth'], row['mag'], row['magType'], row['nst'], row['gap'],
                           row['dmin'], row['rms'], row['net'], row['id'], row['updated'],
                           row['place'], row['type']))
            counter = counter + 1
            if counter%500 == 0:
                cursor.executemany(insertQuery, entriesList)
                db.commit()
                del entriesList[:]

        if counter%500 != 0:
            cursor.executemany(insertQuery, entriesList)
            db.commit()
            del entriesList[:]
            
        cursor.close()    
        gcsFile.close()
        

app = webapp2.WSGIApplication([
    webapp2.Route('/app', handler=MainPage),
], debug=True)

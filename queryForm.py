import cgi,cgitb 
import datetime
import webapp2
import jinja2
import MySQLdb
import os

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)
   
_INSTANCE_NAME = 'assignment1-973:earthquakedata'

class GetPage(webapp2.RequestHandler):
	def get(self): 
		#for debugging
		
		cgitb.enable()   
		form = cgi.FieldStorage()  
		magnitude = form.getvalue('mag') 
		weeklyData = [];  
		
		if (os.getenv('SERVER_SOFTWARE') and os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
			db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='Earthquake', user='root',passwd='root')
	
		cursor = db.cursor()
		cursor.execute('SELECT place FROM Earthquake.MagnitudeData WHERE mag=%s',(magnitude))
        
		for row in cursor.fetchall():
			weeklyData.append(dict([('place',row[0])]))
	
		db.close()
		variables = {'magTwoEntries': weeklyData}
		template = JINJA_ENVIRONMENT.get_template('queryForm.html')
		self.response.write(template.render(variables))
		


app = webapp2.WSGIApplication([
    webapp2.Route('/', handler=GetPage),
], debug=True)

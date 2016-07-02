#Name: Abhinav Joshi
#Course Number: CSE 6331 Section 004
#Lab Number: 2 Upload File Module
'''Copyright (c) 2015 HG,DL,UTA
   Python program runs on local host, uploads, downloads, encrypts local files to google.
   Please use python 2.7.X, pycrypto 2.6.1 and Google Cloud python module '''

#import statements.
import argparse
import httplib2
import os
import sys
import json
import time
import datetime
import io
#Google apliclient (Google App Engine specific) libraries.
from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from apiclient.http import MediaFileUpload


#Mimetype to use if one can't be guessed from the file extension.
_DEFAULT_MIMETYPE = 'application/octet-stream'

#Name of your google bucket.
_BUCKET_NAME = 'assignmentcloud1' 
_API_VERSION = 'v1'

#Chunk size used for uploads and downloads
_CHUNK_SIZE = 2 * 1024 * 1024

#Parser for command-line arguments.
parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])


#client_secret.json is the JSON file that contains the client ID and Secret.
#You can download the json file from your google cloud console.
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

#Set up a Flow object to be used for authentication.
#Add one or more of the following scopes. 
#These scopes are used to restrict the user to only specified permissions (in this case only to devstorage) 
FLOW = client.flow_from_clientsecrets(CLIENT_SECRETS,
  scope=[
      'https://www.googleapis.com/auth/devstorage.full_control',
      'https://www.googleapis.com/auth/devstorage.read_only',
      'https://www.googleapis.com/auth/devstorage.read_write',
    ],
    message=tools.message_if_missing(CLIENT_SECRETS))

#Puts a object into file after encryption and deletes the object from the local PC.
#Parts of put method referenced from - https://code.google.com/p/google-cloud-platform-samples/source/browse/file-transfer-json/chunked_transfer.py?repo=storage
def put(service):
    #User inputs the file name that needs to be uploaded.
    file_name = raw_input('Enter file name to be uploaded to Cloud:\n')
    #Encrypt the given file using AES encryption
    if not file_name or not os.path.isfile(file_name):
        print 'Invalid file name or file not found. Terminating!'
        return
        
    directory, f_name = os.path.split(file_name)
    #Upload the file to Bucket
    try:
        media = MediaFileUpload(file_name, chunksize=_CHUNK_SIZE, resumable=True)
        if not media.mimetype():
            media = MediaFileUpload(file_name, _DEFAULT_MIMETYPE, resumable=True)
        request = service.objects().insert(bucket=_BUCKET_NAME, name=f_name,
                                           media_body=media)

        response = None
        start = datetime.datetime.now()
        while response is None:
            status, response = request.next_chunk()
            if status:
                print "Uploaded %d%%." % int(status.progress() * 100)
            print "Upload Complete!"

        end = datetime.datetime.now()
        duration = end - start
        print ('Upload took {} seconds'.format(duration.seconds))
        #Removes references to the uploaded file
        media = request = None

    except client.AccessTokenRefreshError:
        print ("Error in the credentials")
    
	
def main(argv):
    #Parse the command-line flags.
    flags = parser.parse_args(argv[1:])

    #sample.dat file stores the short lived access tokens, which your application requests user data, attaching the access token to the request.
    #so that user need not validate through the browser everytime. This is optional. If the credentials don't exist 
    #or are invalid run through the native client flow. The Storage object will ensure that if successful the good
    #credentials will get written back to the file (sample.dat in this case). 
    storage = file.Storage('sample.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(FLOW, storage, flags)

    #Create an httplib2.Http object to handle our HTTP requests and authorize it
    #with our good Credentials.
    http = httplib2.Http()
    http = credentials.authorize(http)

    #Construct the service object for the interacting with the Cloud Storage API.
    service = discovery.build('storage', _API_VERSION, http=http)
    put(service)


if __name__ == '__main__':
    main(sys.argv)
# [END all]

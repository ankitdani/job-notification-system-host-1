from http.server import BaseHTTPRequestHandler, HTTPServer
from jsonrpclib import Server
import time
import threading
import cgi
import random
import http.client
import json
import re



host = 'jooble.org'
key = '290e9824-d65f-4b8c-94cc-c2cba6e290f5'

#request headers
connection = http.client.HTTPConnection(host)
headers = {"Content-type": 'application/json,text/javascript, */*',
'Content-Type': 'application/x-www-form-urlencoded'}

'''------------------------------
Function : jobposting
Func to retrive job data from api
---------------------------------
'''
def jobposting():

    body = '{ "keywords": "Computer",}'
    connection.request('POST','/api/' + key, body, headers)
    response = connection.getresponse()
    title_data = []
    job_location = []
    job_description = []
    print(response.status, response.reason)
    data = response.read()
        #print(data)
    data_load = json.loads(data)
    regex = r'[\r\n\t\s]+|&nbsp;|<b>|</b>'

        #print("----------------------")
    for i in data_load["jobs"]:
        title_data.append(i["title"])
            #print("job title:",i["title"])
            #print("job location:",i["location"])
        job_location.append(i['location'])
        i["snippet"] = re.sub(regex, ' ', i["snippet"])
        job_description.append(i["snippet"])
        

    return (title_data, job_location,job_description)

'''--------------------------------------
func: class to process the Post and Get requests
-----------------------------------------'''
class requestAccept(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        self.wfile.write(bytes(message, "utf8"))
    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD': 'POST'}
        )
        thread = 'Ankit'
        button = 'USA'
        jobID = 3 
        self.send_response(200)
        self.end_headers()
        threadName = "Sender : " + thread +" on " + button
        driver = Sender(thread , button, threadName,jobID )
        driver.start()
'''------------------------------------
class to prepare the data parameters
---------------------------------------'''

class Sender(threading.Thread):
    '''--------------------------------------
    func : init
    initialize the required parameters
    -----------------------------------------'''
    def __init__(self, thread, button, threadName,jobID):
        print("Server has started!")
        threading.Thread.__init__(self, name=threadName)
        self.thread = 'Ankit'
        self.button = 'USA'
        self.threadName = threadName
        self.broker = Server('http://localhost:2000')
        self.jobID = jobID
    '''--------------------------------------
    func : run
    prepare to get the  data and provide it to
    the publisher
    -----------------------------------------'''
    def run(self):
        minSalary = 70000
        maxSalary = 100000
        #jobposting = ''
        print(type(minSalary))
        #for counter in range(self.numberMsg):
        message, minSalary, maxSalary = self.getData(minSalary,maxSalary)
        print("message is this", message)
        self.broker.publish(self.button, message)
        time.sleep(0.05)
    '''--------------------------------------
    func : getData
     get the data from api
    -----------------------------------------'''
    
    def getData(self, minSalary, maxSalary):
        data = '{"id":"' + str(self.jobID) + '",'
        if(self.button == "USA"):
        
            y = jobposting() 
            print(type(y))
            job_title = random.choice (y[0])
            job_location = random.choice (y[1])
            job_description = random.choice (y[2])
            data += '"jobTitle":"'+job_title+'","jobLocation":"'+job_location+'","jobDescription":"'+job_description+'",'
            minSalary = (random.randrange(minSalary, 100000))
            print(minSalary)
            maxSalary = (random.randrange(maxSalary, 120000))
            data += '"salary1":"' + str(minSalary) + '","salary2":"' + str(maxSalary)
            data +='"}'
            return data,minSalary,maxSalary


with HTTPServer(('', 5500), requestAccept) as server:

    server.serve_forever()

if __name__== "__main__":
    job.jobposting()

from http.server import BaseHTTPRequestHandler, HTTPServer
from jsonrpclib import Server

from urllib.parse import urlparse
from urllib.parse import parse_qs

import http.server
import socket
import time
import threading
import json
'''connect over localhost for internal comm'''
ip = "0.0.0.0"
'''Default port for client'''
con= 8000
data = {}
varlist = {}
def utf8len(s):
    return len(s.encode('utf-8'))

class StartClass(threading.Thread):
    '''---------------------------------------------
       Function to initialize the client
    -----------------------------------------------'''
    def __init__(self, chanelName, thread):
        self.chanelName = chanelName
        self.broker = Server('http://0.0.0.0:2000')
        self.thread = thread
        self.message_queue = self.broker.wantInfo(thread, chanelName)
    '''---------------------------------------------
       Function to start the client threads
    -----------------------------------------------'''
    def sendInfo(self, client):
        global data
        data.setdefault(self.thread, [])
        while True:
            info = self.broker.waitforMessage(self.thread, self.chanelName)
            if info is not None:
                data[self.thread].append(info)
                time.sleep(0.1)
                if info['data'] == 'End':
                    break
            else:
                time.sleep(0.2)
    
    def alertoff(self):
        self.broker.alertoff_(self.thread, self.chanelName)

class SplitterClass(object):

    '''--------------------------------
    Function that marks the starting of this class
    ----------------------------------'''
    def __init__(self):
        print("Splitter Class Begins")
    
    '''-------------------------------------
    Create socket and spawn threads
    ----------------------------------------'''
    def connection(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            s.bind((ip, con))
            while True:
                s.listen()
                connection, address = s.accept()
                t = threading.Thread(target=self.JagatRaha, args=(connection, address))
                t.start()
    '''-------------------------------------------
    Function to split and decode the REST request
    ----------------------------------------------'''
    def JagatRaha(self, connection, address):
        len = 1024
        with connection:
            connection.settimeout(5)
            while True:
                try:
                    global varlist
                    decodeClient = connection.recv(len).decode()
                    X = decodeClient.split('\r\n')
                    REST  = X[0].split()
                    print("print rest api", REST)
                    splitter = ''

                    #check what data is present in REST and if it is a /subscribe request
                    if "/wantInfo" in REST[1]: 
                        Y = REST[1].split('?')
                        splitPara = Y[1].split("=")
                        splitName = splitPara[1].split('&')
                        splitName = splitName[0]
                        buttonName = splitPara[2]
                        splitter = StartClass(buttonName, splitName)
                        if(splitName not in varlist):
                            varlist[splitName] = {}
                        varlist[splitName][buttonName] = splitter
                        splitter.sendInfo(connection)
                    
                    #check what data is present in REST and if it is a /unsubscribe request
                    elif "/unsubscribe" in REST[1]:
                        Y = REST[1].split('?')
                        splitPara = Y[1].split("=")
                        splitName = splitPara[1].split('&')
                        splitName = splitName[0]
                        buttonName = splitPara[2]
                        if(splitName in varlist):
                            if(buttonName in varlist[splitName]):
                                varlist[splitName][buttonName].alertoff()

                    #check what data is present in REST and if it is a /getData request
                    elif "/getData" in REST[1]:
                        Y = REST[1].split('?')
                        splitPara = Y[1].split("=")
                        splitName = splitPara[1]
                        self.getData(connection,splitName)



                except Exception as e: #print exception 
                    print(e)
                    break
        connection.close()
    '''-------------------------------------------
    Function to get corret data and encode it
    We need to see if a valid json string is received
    so that it ensures correct encoding.
    ----------------------------------------------'''
    def getData(self,connection,splitName):

        global data
        print("this is data ", data)

        json_string = json.dumps(data[splitName])
        connection.sendall(str.encode("HTTP/1.1 200 OK\n",'iso-8859-1'))
        connection.sendall(str.encode('Content-Type: application/json\n', 'iso-8859-1'))
        connection.sendall(str.encode('Access-Control-Allow-Origin: *\n', 'iso-8859-1'))
        connection.sendall(str.encode('\r\n'))
        connection.sendall(json_string.encode())
    '''-------------------------------------------
    Main driver func to call the splitter class
    ----------------------------------------------'''
def main():
    SplitterClass().connection()

if __name__ == '__main__':  
    main()
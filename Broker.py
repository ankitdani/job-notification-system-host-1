import warnings
from multiprocessing.pool import ThreadPool
from threading import Lock
from threading import Thread
from queue import Queue, PriorityQueue, Empty
from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer
collection = ThreadPool(processes=1)


class MainProcessing():
    '''______________________________________
     Function : init for class initialization
    _________________________________________
    '''
    def __init__(self, Qnum=100, Qnumcal=2**31):
        self.Qnum = Qnum
        self.Qnumcal = Qnumcal

        self.button = {}
        self.count = {}

        self.button_lock = Lock()
        self.count_lock = Lock()
    '''______________________________________
     Function : publish
     function to process the message queues
    _________________________________________
    '''
    def publish_(self, road, message, checkIfPQ, priority):
        if priority < 1 or not road or not message:
            raise ValueError('priority, road, and message cannot be None or less than 1')
        if road not in self.button:
            with self.button_lock:
                if road not in self.button:
                    self.button[road] = {}
        with self.count_lock:
            self.count[road] = (self.count.get(road, 0) + 1) % self.Qnumcal
            _id = self.count[road]
        for listn in self.button.get(road, {}):
            buttonQueue = self.button[road][listn]
            if buttonQueue.qsize() >= self.Qnum:
                warnings.warn(f"Queue overflow for channel {road}, > {self.Qnum} (self.Qnum parameter)")
            else:
                data = {'channel': road, 'data': message, 'id': _id} if not checkIfPQ else (priority, dictionaryforOrdering(data=message, id=_id))
                buttonQueue.put(data, block=False)
    '''______________________________________
     Function : alertoff
     process the alertoff action
    _________________________________________
    '''
    def alertoff(self, ner, road, isInfo):
        if not road or isInfo:
            raise ValueError('Do not enter any values')

        if road in self.button:
            self.button[road][ner].remove(isInfo)
    '''______________________________________
     Function : alerton_
     To process the subscription by acquiring locks
    _________________________________________
    '''
    def alerton_(self, ner, road, checkIfPQ):
        if not road:
            raise ValueError('road : None value not allowed')
        with self.button_lock:
            self.button.setdefault(road, {})
            isInfo = ImportantOrder(self, road) if checkIfPQ else DataCheck(self, road)
            self.button[road][ner] = isInfo
        return isInfo
 
    '''---------------------------------
    func : InfoQ 
    check the channel is available
    ------------------------------------
    '''
    def InfoQ(self, ner, buttonName):
        if self.button[buttonName][ner].empty():
            return None
        return self.button[buttonName][ner]
 
    '''---------------------------------
    func : DataCheck 
    Check if the data in the message queue is valid
    ------------------------------------
    '''
class ImportantOrder(PriorityQueue):

    def __init__(self, parent, road):
        super().__init__()
        self.parent = parent
        self.name = road

    def waitforMessage(self, block=True, timeout=None):
        while True:
            try:
                impData = self.get(block=block, timeout=timeout)
                assert isinstance(impData, tuple) and \
                       len(impData) == 2 and \
                       isinstance(impData[1], dict) and \
                       len(impData[1]) == 2, "Bad data in chanel queue !"
                yield impData[1]
            except Empty:
                return

    def alertoff(self):
        self.parent.unsubscribe(self.name, self)

'''---------------------------------
    func : DataCheck 
    Check if the data in the message queue is valid
------------------------------------
    '''
class DataCheck(Queue):
    def __init__(self, parent, road):
        super().__init__()
        self.parent = parent
        self.name = road

    def waitforMessage(self, block=True, timeout=None):
        while True:
            try:
                infoD = self.get(block=block, timeout=timeout)
                assert isinstance(infoD, dict) and len(infoD) == 3,\
                       "Bad data in chanel queue !"
                yield infoD
            except Empty:
                return

    def alertoff(ner, buttonName, self):
        self.parent.alertoff(ner, buttonName, self)

'''___________________________________
class to handle the pubsub model
______________________________________
'''
class PublishSubscribe(MainProcessing):
    

    #define the functions to be used 
    def getMessageQueue(self, ner, buttonName):
        return self.InfoQ(ner, buttonName)

    def wantInfo(self, ner, road):
        return self.alerton_(ner, road, False)    


    def publish(self, road, message):
        self.publish_(road, message, False, priority=100)

'''------------------------------------
create a disctionary that will help to 
order the id
----------------------------------------
'''
class dictionaryforOrdering(dict):
    def __lt__(self, other):
        return self['id'] < other['id']

#create an object of class to use further

object_of_class = PublishSubscribe()



'''----------------------------------
create a queue for listner thread 
-------------------------------------'''
def threadwaitforMessage(ner, buttonName):
    global object_of_class
    isInfo = object_of_class.getMessageQueue(ner, buttonName)
    if isInfo == None:
        return None
    message = next(isInfo.waitforMessage())
    return message


'''----------------------------------
create a listner thread 
-------------------------------------'''
def waitforMessage(ner, chanelName):
    global collection
    thread = collection.apply_async(threadwaitforMessage, (ner,chanelName))
    message = thread.get()
    return message


'''----------------------------------
create a msg queue for the thread
-------------------------------------'''
def threadSub(ner, buttonName):
    global object_of_class
    print("this is subscribe", ner, buttonName)
    object_of_class.wantInfo(ner, buttonName)
    return ner
'''----------------------------------
create a subscriber thread 
-------------------------------------'''
def wantInfo(ner, chanelName):
    print("this is wantInfo")
    thread = Thread(target = threadSub, args = (ner, chanelName))
    thread.start()
    thread.join()


'''----------------------------------
create a msg queue for the thread
-------------------------------------'''
def threadPub(buttonName, message):
    global object_of_class
    object_of_class.publish(buttonName, message)
    #print("road name" , buttonName, "message is ", message)
'''----------------------------------
create a publish thread 
-------------------------------------'''
def publish(buttonName, message):
    thread = Thread(target = threadPub, args = (buttonName, message))
    thread.start()
    thread.join() 


'''----------------------------------
create a msg queue for the thread
-------------------------------------'''
def unthreadSub(ner,buttonName):
    global object_of_class
    isInfo = object_of_class.getMessageQueue(ner, buttonName)
    isInfo.unsubscribe(ner,buttonName)
'''----------------------------------
create a thread 
-------------------------------------'''
def unsubscribe_(ner, chanelName):
    thread = Thread(target = unthreadSub, args = (ner, buttonName))
    thread.start()
    thread.join()

'''----------------------
Main : create an internal server
and register the requests
-------------------------
'''
def main():
    commonConnector = SimpleJSONRPCServer(('localhost', 2000))
    commonConnector.register_function(publish)
    commonConnector.register_function(wantInfo)
    commonConnector.register_function(waitforMessage)
    commonConnector.register_function(unsubscribe_)
    print("Connector started")
    commonConnector.serve_forever()

if __name__ == '__main__':
    main()
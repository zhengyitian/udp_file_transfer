import socket,select,uuid
import json,binascii,hashlib
import time,os,sys,platform
from collections import deque
import random

def calMd5(st):    
    co = 0
    m = hashlib.md5()
    f = open(st,'rb')
    while True:
        s = f.read(10*1024*1024)
        if len(s) == 0:
            break
        co += len(s)
        m.update(s)             
    f.close()
    return m.hexdigest()     

timeoutTime = 0.55
fileSize = 500*1024*1024
serverIp = '155.138.174.74'
#serverIp = '127.0.0.1'
portList = list(range(20000,20500))
salt = b'salt'

def makePack(s,salt):
    u = str(uuid.uuid1())
    u = u.replace('-','')
    u2 = binascii.unhexlify(u)
    s1 = u2+s
    dk = hashlib.pbkdf2_hmac('md5', s1, salt, 2)
    s2 = s1+dk
    return u,s2

def checkPackValid(s,u,salt):
    if len(s)<16:
        return b'',False
    s1 = s[-16:]
    s2 = s[:-16]
    uuid = binascii.unhexlify(u)
    dk = hashlib.pbkdf2_hmac('md5', s2, salt, 2)
    if dk != s1:
        return b'',False
    if s2[:16] != uuid:
        return b'',False
    return s2[16:],True

class fileClient():
    def __init__(self):
        self.fileSize = fileSize
        self.packSize = 1450
        self.packNum = int(self.fileSize/self.packSize)+1
        self.f = open('b','wb')        
        self.gotNum = 0
        self.lostNum = 0
        self.maxRec = 0    
        self.timeoutTime = timeoutTime
        self.minRec = timeoutTime     
        self.readyList = deque()
        self.workingMap = {}
        self.workingSet = set()
        self.recNum = -1
        self.finishSet = set()
        self.finishMap = {}
        self.writeCache = []
        self.nextPack = 0

    def get(self): 
        end = self.packSize*self.recNum      
        fileId = str(uuid.uuid1())
        if self.readyList:
            num = self.readyList.popleft()
            self.workingSet.add(num)
            self.workingMap[fileId] = num
            return fileId,end,num*self.packSize,self.packSize
        
        if self.nextPack > self.packNum:
            if self.workingSet:
                maxNum = random.randint(min(self.workingSet),max(self.workingSet)) 
            else:
                return fileId,end,0,0
        else:
            maxNum = self.nextPack
            self.nextPack += 1
            
        self.workingSet.add(maxNum)
        self.workingMap[fileId] = maxNum
        return fileId,end,maxNum*self.packSize,self.packSize                  
    
    def push(self,id,data):
        self.gotNum += 1     
        if id not in self.workingMap:
            return
        num = self.workingMap[id]
        del self.workingMap[id]    
        if num not in self.workingSet:
            return
        self.workingSet.remove(num)
        self.finishSet.add(num)
        self.finishMap[num] = data
        if num!=self.recNum+1:
            return
        while self.recNum+1 in self.finishSet :
            self.recNum += 1
            self.writeCache.append(self.finishMap[self.recNum])
            if len(self.writeCache)>1000:
                self.f.write(b''.join(self.writeCache))
                self.writeCache = []
            
            del self.finishMap[self.recNum]
            self.finishSet.remove(self.recNum)
            if self.recNum==self.packNum:
                self.f.write(b''.join(self.writeCache))
                self.f.close()
                md5 = calMd5('b')
                print ('md5 : '+md5)
                sys.exit(0)
    
    def lost(self,id):
        self.lostNum += 1      
        if id not in self.workingMap:
            return
        num = self.workingMap[id]
        del self.workingMap[id]
        if num not in self.workingSet:
            return             
        self.workingSet.remove(num)
        self.readyList.append(num)        
    
    def clearStat(self):
        self.lostNum=self.gotNum = 0
        self.maxRec = 0        
        self.minRec = timeoutTime             
        
gFile = fileClient()    
sockMap = {}
platformName = platform.system()
pyV = sys.version_info[0]
def getRunningTime():    
    if pyV == 3:
        return time.monotonic()
    if platformName=='Windows':
        return time.clock()
    elif platformName=='Linux':
        with open('/proc/uptime') as f:
            return float(f.read().split()[0])
    else:
        return time.time()
    
for i in portList:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
    sockMap[sock] = {'num':i,'createTime':getRunningTime()-2*timeoutTime,'fileId':str(uuid.uuid1())}  

def newData(sock,newSock=False):  
    num = sockMap[sock]['num']
    fileId = sockMap[sock]['fileId']
    if newSock:    
        sock.close()
        gFile.lost(fileId)
        del sockMap[sock]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    fileId,end,pos,len = gFile.get()
    m = {'end':end,'pos':pos,'len':len}
    j = json.dumps(m)
    j = j.encode()
    u ,s2 = makePack(j,salt)    
    sock.sendto(s2, (serverIp, num))    
    sockMap[sock] = {'num':num,'createTime':getRunningTime(),'uuid':u,'fileId':fileId}

def deal_rec(l):
    for sock in l:
        j = sock.recv(10000)
        u = sockMap[sock]['uuid']
        s2,sign = checkPackValid(j,u,salt)
        if not sign:
            newData(sock,True)
            continue 
        gFile.push(sockMap[sock]['fileId'],s2)
        recTime = getRunningTime()-sockMap[sock]['createTime']
        if recTime>gFile.maxRec:
            gFile.maxRec = recTime
        if recTime<gFile.minRec:
            gFile.minRec = recTime
        newData(sock)
        
def deal_timeout():
    for sock in list(sockMap.keys()):
        v = sockMap[sock]
        if v['createTime']+gFile.timeoutTime<getRunningTime():
            newData(sock,True)
            
staTime = getRunningTime()  
startSign = []
startTime = getRunningTime()

while True:  
    if not startSign:
        r = select.select(sockMap.keys(),[],[],gFile.timeoutTime)  
    else:
        startSign.append(1)
    deal_rec(r[0])      
    deal_timeout()    
    if getRunningTime()-staTime>1:
        staTime = getRunningTime()
        if gFile.maxRec>0:
            gFile.timeoutTime = min(gFile.maxRec+0.1,timeoutTime)
        ss = '%s,%s,%2.3f ,%2.3f,%s/%s,%2.3f,%s'%(gFile.gotNum,gFile.lostNum,gFile.maxRec,gFile.minRec,\
                                         gFile.recNum,gFile.packNum,gFile.recNum*1.0/gFile.packNum,\
                                         int((gFile.packNum-gFile.recNum)/(gFile.gotNum+1)))
        s2 = ' ## %s,%s'%(len(gFile.workingMap),len(gFile.finishMap))        
        print(int(getRunningTime()-startTime),ss+s2)
        gFile.clearStat()
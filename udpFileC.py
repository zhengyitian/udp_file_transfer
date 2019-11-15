import socket,select
import struct,binascii,hashlib
import time,os,sys,platform
from collections import deque
import random

timeoutTime = 0.55
fileSize = 594280448
serverIp = '155.138.174.74'

baseNum = 20000
portNum = 20500
bigNum = 21000
packSize = 6000
salt = b'salt'

platformName = platform.system()
pyV = sys.version_info[0]

def getRunningTime():    
    if platformName=='Windows':
        return time.clock()
    if pyV == 3:
        return time.monotonic()    
    if platformName=='Linux':
        with open('/proc/uptime') as f:
            return float(f.read().split()[0])
    else:
        return time.time()
    

def calMd5(st):    
    m = hashlib.md5()
    f = open(st,'rb')
    while True:
        s = f.read(10*1024*1024)
        if len(s) == 0:
            break
        m.update(s)             
    f.close()
    return m.hexdigest()     

def makePack(s,salt):
    u = gFile.getuuid()
    u2 = binascii.unhexlify(u)
    s1 = u2+s
    dk = salt[-4:]
    s2 = s1+dk
    return u,s2

def checkPackValid(s,u,salt):
    if len(s)<4:
        return b'',False
    s1 = s[-4:]
    s2 = s[:-4]
    uuid = binascii.unhexlify(u)
    dk = salt[-4:]
    if dk != s1:
        return b'',False
    if s2[:4] != uuid:
        return b'',False
    return s2[4:],True


class fileClient():
    def __init__(self):
        self.base = 0
        self.fileSize = fileSize
        if os.path.exists('b'):
            st = os.stat('b')
            self.fileSize = fileSize-st.st_size
            self.base = st.st_size
        self.packSize = packSize
        self.packNum = int(self.fileSize/self.packSize)+1
        self.f = open('b','ab')        
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
        self.uuid = 0
        self.timeoutList = []

    def getuuid(self):
        self.uuid += 1
        h = hex(self.uuid)[2:]        
        return '0'*(8-len(h))+h

    def get(self): 
        end = self.packSize*self.recNum      
        fileId = self.getuuid()
        if self.readyList:
            num = self.readyList.popleft()
            self.workingSet.add(num)
            self.workingMap[fileId] = num
            return fileId,end+self.base,num*self.packSize+self.base,self.packSize
        
        if self.nextPack > self.packNum:
            if self.workingSet:
                maxNum = random.randint(min(self.workingSet),max(self.workingSet)) 
            else:
                return fileId,end+self.base,self.base,0
        else:
            maxNum = self.nextPack
            self.nextPack += 1
            
        self.workingSet.add(maxNum)
        self.workingMap[fileId] = maxNum
        return fileId,end+self.base,maxNum*self.packSize+self.base,self.packSize                  
    
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
    def close(self):
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
portList = list(range(20000,portNum))
cacheList = deque(range(portNum,bigNum))
for i in range(20000,bigNum):
    gFile.timeoutList.append(float('inf'))
    
for i in portList:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
    sockMap[sock] = {'num':i,'createTime':getRunningTime()-2*timeoutTime,'fileId':gFile.getuuid()}  
    

def newData(sock,newSock=False):  
    num = sockMap[sock]['num']
    fileId = sockMap[sock]['fileId']
    if newSock:    
        cacheList.append(num)
        gFile.timeoutList[num-baseNum] = float('inf')
        num = cacheList.popleft()
        sock.close()
        gFile.lost(fileId)
        del sockMap[sock]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    fileId,end,pos,len = gFile.get()
    j = b''
    j += struct.pack('q',end)
    j += struct.pack('q',pos)
    j += struct.pack('q',len)   
    u ,s2 = makePack(j,salt)    
    sock.sendto(s2, (serverIp, num))    
    sockMap[sock] = {'num':num,'createTime':getRunningTime(),'uuid':u,'fileId':fileId}
    gFile.timeoutList[num-baseNum] = getRunningTime()

def deal_rec(l):
    for sock in l:
        j = sock.recv(100000)
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
    hasOut = False
    for sock in list(sockMap.keys()):
        v = sockMap[sock]
        if v['createTime']+gFile.timeoutTime<getRunningTime():
            newData(sock,True)
            hasOut = True
    return hasOut
            

def main():
    staTime = getRunningTime()  
    startSign = []
    startTime = getRunningTime()    
    
    while True:  
        j =oriTime=overTime= 0
        if  startSign:
            tempM = min(gFile.timeoutList)
            wt = gFile.timeoutTime+tempM-getRunningTime()
            for i in range(baseNum,bigNum):
                if gFile.timeoutList[i-baseNum]==tempM:
                    j = i
            wt = max(0,wt)
            r = select.select(sockMap.keys(),[],[],wt)  
        else:
            startSign.append(1)
            r = [[],]
        deal_rec(r[0])      
        hasOut = deal_timeout()  
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
try:
    main()
except KeyboardInterrupt:
    gFile.close()
    
    
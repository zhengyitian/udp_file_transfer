import socket,select,platform
import os,struct,hashlib,binascii
platformName = platform.system()
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

salt = b'salt'
cacheSize = 1024*1024*60
st = os.stat('a')
print(st.st_size)
md5 = calMd5('a')
print ('md5 : '+md5)
bigNum = 20500
if platformName=='Linux':
    bigNum = 20700
listenPort = list(range(20000,bigNum))
sockMap = {}
 

def checkPackValid_server(s,salt):
    if len(s)<4:
        return '',b''
    s1 = s[-4:]
    s2 = s[:-4]
    dk = salt[-4:]
    if dk != s1:
        return '',b''
    if len(s2)<4:
        return '',b''
    return (binascii.hexlify(s2[:4])).decode() ,s2[4:]

def makePack_server(s,u,salt):
    u2 = binascii.unhexlify(u)
    s1 = u2+s
    dk = salt[-4:]
    s2 = s1+dk
    return s2


for i in listenPort:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0',i))
    sockMap[sock] = {}
    
class fileWrapper():
    def __init__(self):
        self.f = open('a','rb')
        self.readCache = self.f.read(cacheSize)
        self.beginPos = 0
        self.co = 0

    def refresh(self,end):
        if end<0:
            return
        if end > self.beginPos+cacheSize/2 or end<self.beginPos :
            self.f.seek(end)          
            self.readCache = self.f.read(cacheSize)
            self.beginPos = end             
    
    def get(self,pos,len):
        if len==0:
            return b''
        if pos + len >=self.beginPos+cacheSize or pos<self.beginPos:
            print(self.co,'cache error')
            self.co += 1
            self.f.seek(pos)
            return self.f.read(len)            
        return self.readCache[pos-self.beginPos:pos+len-self.beginPos]

gFile = fileWrapper()

def deal_rec(l):
    re = []
    reSocks = []
    for one in l:
        data, addr = one.recvfrom(100000)
        uuid ,ss = checkPackValid_server(data,salt)
        if not uuid :
            continue         
        end = struct.unpack('q',ss[:8])[0]
        pos = struct.unpack('q',ss[8:16])[0]
        len = struct.unpack('q',ss[16:24])[0]
        j = gFile.get(pos,len)
        gFile.refresh(end)
        data = makePack_server(j, uuid,salt)
        one.sendto(data,addr)
      
while True:    
    r = select.select(sockMap.keys(),[],[],1)
    deal_rec(r[0])

// udpfileC.cpp : 定义控制台应用程序的入口点。
//

#include "stdafx.h"
#include "stdio.h"
#include <deque>

# ifndef FD_SETSIZE
# define FD_SETSIZE 1024
# endif
#include "winsock2.h"

#pragma comment(lib,"ws2_32.lib")
const int packSize = 10000;

//char serverIp[]="127.0.0.1";
//char serverIp[]="192.168.100.135";
//char serverIp[]="154.221.23.74";
char serverIp[]="155.138.174.74";
char serverIp2[]="144.202.17.72";
//"155.138.174.74"
//192.168.100.135
//154.221.23.74

int staGot=0;
int staOut=0;
int timeoutTime_ori  =400;
int timeoutTime=timeoutTime_ori;
__int64 fileSize = 500*1024*1024;
int recMin = timeoutTime_ori;
int recMax = 0;
int portBase = 20000;
const int portNum = 500;
int bigNum = 21000;

int packLen = 1400;
char uuid[4];
unsigned int selectTime = GetTickCount();
int selectBase = 0;
int sentThisSecond=0;
int sentLimit = 800;
int changeIp=0;
int gotLimit = 100;
unsigned int useIp2 = 0;
std::deque<int> portCache;

char * getuuid()
{
	static int co=0;
	//co++;
	memcpy(uuid,(void *)&co,4);
	return uuid;
}
char packBuffer[packSize];
int packBufferSize =0;

char * makePack(char * msg,int len)
{
	memcpy(packBuffer,uuid,4);
	memcpy(packBuffer+4,msg,len);
	memcpy(packBuffer+4+len,"salt",4);
	return packBuffer;
}

void InitWinsock()
{
	WSADATA wsaData;
	WSAStartup(MAKEWORD(2, 2), &wsaData);
}




SOCKET sockList[portNum];
int portNumList[portNum];
unsigned int creatTimeList[portNum];
int fileIdList[portNum];
int uuidList[portNum];
struct fd_set*  fdSet[portNum];
sockaddr_in* addrList[portNum];




char returnMsg[packSize];
bool checkPackValid(char * msg,char* u,int len)
{
	if (len<8) return false;
	char temp[5]="salt";
	for(int i=0;i<4;i++)
	{

		if (msg[i]!=u[i] || msg[len-1-i]!=temp[3-i]) return false;
	}
	memcpy(returnMsg,msg+4,len-8);
	return true;
}

class fileClient{
	void ini()
	{
		/*
		int n;

		FILE *fptr;
		if ((fptr = fopen("d:\\program.bin","wb")) == NULL){
		printf("Error! opening file");
		// Program exits if the file pointer returns NULL.
		exit(1);
		}
		fseek ( fptr , 0 , SEEK_SET );
		fwrite("ll", 2, 1, fptr); 

		fclose(fptr); 
		*/

	}

};

struct sockaddr_in serverInfo;
int leng = sizeof(serverInfo);

void newData(int index,bool newSock)
{
	int num = portNumList[index];
	bool toSend=true;
	if (sentThisSecond>sentLimit)
	{
		toSend=false;
	}
	if (newSock){
		staOut++;
		if (toSend)
		{
		sentThisSecond++;
		closesocket(sockList[index]);
		sockList[index] = socket(AF_INET,SOCK_DGRAM,IPPROTO_UDP);}
portCache.push_back(num);
num = portCache.at(0);
		 portCache.pop_front();
	}
	else
	{
		staGot++;
	}
	char data[24];
	static __int64 pos=0;
	//pos += 1000;
	__int64 len=packLen;
	__int64 end = pos;
	memcpy(data,(void *)&end,8);
	memcpy(data+8,(void *)&pos,8);
	memcpy(data+16,(void *)&len,8);
	char * id = getuuid();
	int temp;
	memcpy((void*)&temp,id,4);

	char *msg = makePack(data,24);

	(*addrList[index]).sin_family = AF_INET;
	
	(*addrList[index]).sin_port = htons(num);
	if (changeIp>0)
	{
		(*addrList[index]).sin_addr.s_addr = inet_addr(serverIp2);
		useIp2=GetTickCount();
	}
	else{
	(*addrList[index]).sin_addr.s_addr = inet_addr(serverIp);
	}
	creatTimeList[index]=GetTickCount();
	uuidList[index]=temp;

	if (toSend)
	{
	
	if (sendto(  sockList[index], msg, 32, 0, (sockaddr*)addrList[index], leng) == SOCKET_ERROR)
	{
		printf("sendto wrong");
		exit(0);
	}
	}




}
struct fd_set fds2;

char recBuffer[packSize];

void deal_rec()
{
	for (int i=0;i<portNum;i++)
	{
		if (!FD_ISSET(sockList[i],&fds2)) 
		{continue;}

		int retLen = recvfrom(sockList[i], recBuffer, packSize, 0, (sockaddr*)&serverInfo, &leng);
		if ( retLen== SOCKET_ERROR)
		{
			printf("recvfrom wrong");
			exit(0);
		}
		int temp = uuidList[i];
		if (!checkPackValid(recBuffer,(char *)&temp,retLen))
		{
			newData(i,true);
			continue;
		}
		int recTime = GetTickCount()-creatTimeList[i];
		if(recTime>recMax)
			recMax=recTime;
		if (recTime<recMin)
			recMin=recTime;
		newData(i,false);
	}

}


void deal_timeout()
{
	for (int i=0;i<portNum;i++)
	{
		if(creatTimeList[i]+timeoutTime<GetTickCount())
		{
			newData(i,true);
		}

	}
}

int getMinTime()
{
	int minTime = GetTickCount();
	for (int i=0;i<portNum;i++)
	{
	if(creatTimeList[i]<minTime)
minTime=creatTimeList[i];
	}
	return minTime;
}
int _tmain(int argc, _TCHAR* argv[])
{
	for (int i =portBase+portNum;i<bigNum;i++)
	{
		portCache.push_back(i);
	}
	int staTime=GetTickCount();
	InitWinsock();
	for (int i=0;i<portNum;i++)
	{

		addrList[i] = new sockaddr_in();
		sockList[i] = socket(AF_INET,SOCK_DGRAM, IPPROTO_UDP);
		portNumList[i]=portBase+i;
		creatTimeList[i] = GetTickCount()-2*timeoutTime;
		fileIdList[i] = -1;
		struct fd_set* fds2 = new struct fd_set();
		fdSet[i]=fds2;
	}

	struct timeval timeout2;

	while(1)
	{
		timeout2.tv_sec = 0;
		int wt = getMinTime();
		wt = timeoutTime+wt-GetTickCount();
		if (wt<0)
			wt=0;
		timeout2.tv_usec = 1000*wt;
		FD_ZERO(&fds2);

	for (int i=0;i<portNum;i++)
	{
FD_SET(sockList[i],&fds2);
	}

	int r = select(0,&fds2, 0, 0, &timeout2);

if (r>0)
{deal_rec();}
	if(0)
	{
		timeout2.tv_sec = 0;
		timeout2.tv_usec = 0;
	
		for (int i=0;i<portNum;i++)
		{

			FD_ZERO(fdSet[i]);

			FD_SET(sockList[i],fdSet[i]);


			int ret = select(0,fdSet[i], 0, 0, &timeout2);

			if (ret)
			{
				int retLen = recvfrom(sockList[i], recBuffer, packSize, 0, (sockaddr*)&serverInfo, &leng);
				if ( retLen== SOCKET_ERROR)
				{
					printf("recvfrom wrong");
					exit(0);
				}
				int temp = uuidList[i];
				if (!checkPackValid(recBuffer,(char *)&temp,retLen))
				{
					newData(i,true);
					continue;
				}
				int recTime = GetTickCount()-creatTimeList[i];
				if(recTime>recMax)
					recMax=recTime;
				if (recTime<recMin)
					recMin=recTime;
				newData(i,false);
			}
		}
	}

	deal_timeout();

	if (GetTickCount()-staTime>1000)
	{
		static unsigned int startTime = GetTickCount();

		if(changeIp==0&&staGot<gotLimit&&GetTickCount()-startTime>5000&&GetTickCount()-useIp2>3000)
		{
			changeIp=10;
		}

		if (changeIp>0)
		{
			changeIp--;
		}
		
		staTime = GetTickCount();
		printf("%d,%d,%d,%d,%d,%d,%d\n",staGot,staOut,recMin,recMax,selectBase,sentThisSecond,changeIp);
		staGot=staOut=0;
		recMin = timeoutTime_ori;
		recMax = 0;
		if (recMax>0)(timeoutTime=recMax+100);
sentThisSecond=0;
	}

if (GetTickCount()-selectTime>15*1000)
{
	selectTime=GetTickCount();
	if(selectBase==0)
		selectBase=portNum;
	else
selectBase=0;
}

	}



return 0;
}


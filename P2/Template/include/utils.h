#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/msg.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <ctype.h>

#define chunkSize 1024
#define filePathMsgSize 50
#define MSGSIZE 1100
#define ENDTYPE 1000
#define ACKTYPE 1100

#define PERM 0666 // permissions for sending chunk data
#define ENDMSG "END\0" // end message
#define ACKMSG "ACK\0" // ACK

struct msgBuffer {
    long msgType;
    char msgText[MSGSIZE];
};

struct myMsgBuffer {
    long msgType;
    char msgText[chunkSize+1];
};

struct filePathBuffer{
  long msgType;
  char msgText[filePathMsgSize+1];
};

// mapper side
int validChar(char c);
char *getWord(char *chunk, int *i);
char *getChunkData(int mapperID);
void sendChunkData(char *inputFile, int nMappers);


// reducer side
int hashFunction(char* key, int reducers);
int getInterData(char *key, int reducerID);
void shuffle(int nMappers, int nReducers);

// directory
void createOutputDir();
char *createMapDir(int mapperID);
void removeOutputDir();
void bookeepingCode();

#endif

#include "utils.h"

// returns the next chunk of data
char *getChunkData(int mapperID) {
  // open message queue
  key_t key = ftok(".", 5584353); // generating a unique key
  int qid = msgget(key, PERM | IPC_CREAT); // creating the message queue
  // receive chunk from the master
  struct myMsgBuffer chunk;
  chunk.msgType = mapperID;
  memset(chunk.msgText, '\0', chunkSize+1);
  if (msgrcv(qid, &chunk, sizeof(struct myMsgBuffer), mapperID, 0) == -1) {
    printf("failed to receive message in getChunkData() for mapperID %d\n", mapperID);
    exit(0);
  }
  // check for END message and send ACK to master
  char* c = (char *)(malloc(sizeof(char)*(chunkSize+1)));
  strcpy(c, chunk.msgText);
  if(!strcmp(c, ENDMSG)){ // checks if an end message is received
    struct myMsgBuffer ackmsg;
    ackmsg.msgType = ACKTYPE;
    strcpy(ackmsg.msgText, ACKMSG);
    if (msgsnd(qid, &ackmsg, sizeof(struct myMsgBuffer), 0) == -1) { // send an ack message 
      printf("failed to send message in getChunkData() for mapperID %d\n", mapperID);
      exit(0);
    }
    return NULL; // no more chunks to read, return null
  }
  return c; // returns the received chunk text
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
  key_t key = ftok(".", 5584353); // generating a unique key
  int qid;
  if ((qid = msgget(key, PERM | IPC_CREAT)) < 0) { // creating the message queue
    printf("failed to create message queue in sendChunkData()\n");
    exit(0);
  }

  int fd = open(inputFile, O_CREAT | O_RDONLY, 0777); // open the file

  // keeps track of which mapper to send data to in RR fashion
  long onMapper = 1;

  int nread; // keeps track of bytes read
  struct myMsgBuffer chunk; // the buffer used to send chunks of data to the mappers
  memset(chunk.msgText, '\0', chunkSize+1);
  while((nread = read(fd, chunk.msgText, chunkSize)) > 0){ // send messages to queue in RR fashion
    if(nread == chunkSize){ // find the closest space if nread == 1024
      int i = 0;
      while(chunk.msgText[nread+i-1] != ' '){ // loop until a space is found
	       chunk.msgText[nread+i-1] = '\0'; // erase the word that was cut off
	        i--;
      }
      chunk.msgText[nread+i-1] = '\0';
      lseek(fd, i, SEEK_CUR); // go back to space in file
    }
    chunk.msgType = onMapper; // sets msg type to current mapper
    if(msgsnd(qid, &chunk, sizeof(struct myMsgBuffer), 0) == -1){ // sends chunk to mapper
      printf("failed to send message in sendChunkData() for mapperID %ld\n", onMapper);
      exit(0);
    }

    // set to next mapper
    onMapper++;
    if(onMapper > nMappers){ // to ensure round robin fashion
      onMapper = 1;
    }
    memset(chunk.msgText, '\0', chunkSize+1);
  }
  close(fd); // close the file as we have written out all of its contents

  // Send END messages to each mapper
  memset(chunk.msgText, '\0', chunkSize+1);
  strcpy(chunk.msgText, ENDMSG);
  for(int i = 1; i <= nMappers; i++){
    chunk.msgType = i;
    if(msgsnd(qid, &chunk, sizeof(struct myMsgBuffer), 0) == -1){
      printf("Failed to send end message in sendChunkData() for mapperID %d\n", i);
      exit(0);
    }
  }
  // wait for ACK from mappers
  for(int i = 0; i < nMappers; i++){
    if (msgrcv(qid, &chunk, sizeof(struct myMsgBuffer), ACKTYPE, 0) == -1){
      printf("Failed to receive ack message in sendChunkData()\n");
      exit(0);
    }
  }
  // close the message queue
  msgctl(qid, IPC_RMID, 0);
}

// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
  unsigned long hash = 0;
  int c;

  while ((c = *key++)!='\0')
    hash = c + (hash << 6) + (hash << 16) - hash;

  return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
  // open message queue
  key_t k = ftok(".", 5584353); // generating a unique key
  int qid;
  if ((qid = msgget(k, PERM | IPC_CREAT)) < 0) { // creating the message queue
    printf("failed to create message queue in getInterData()\n");
    exit(0);
  }
  // receive chunk from the master
  struct filePathBuffer f;
  f.msgType = reducerID;
  memset(f.msgText, '\0', filePathMsgSize+1);
  if (msgrcv(qid, &f, sizeof(struct filePathBuffer), reducerID, 0) == -1) {
    printf("failed to receive message in getInterData() for reducerID %d\n", reducerID);
    exit(0);
  }
  // check for END message and send ACK to master
  if(!strcmp(f.msgText, ENDMSG)){
    f.msgType = ACKTYPE;
    memset(f.msgText, '\0', filePathMsgSize+1);
    strcpy(f.msgText, ACKMSG);
    if (msgsnd(qid, &f, sizeof(struct filePathBuffer), 0) == -1) {
      printf("failed to send ack message in getInterData() from reducerID %d\n", reducerID);
      exit(0);
    }
    return 0; // no more file paths to receive
  }else{
    strcpy(key, f.msgText); // copy file path to the key
    return 1; //  more paths to receive
  }
}

// Goes through a directory and sends msgs to reducers about file paths to .txt files
int traverseDirectory(int mapperID, int qid, int nReducers){
  // building general path for mapout
  char path[50] = "output/MapOut/Map_";
  char id[5];
  sprintf(id, "%d/", mapperID);
  strcat(path, id);

  // opening directory
  DIR* dir = opendir(path);
  if(dir==NULL){
    printf("The path passed is invalid in traverseDirectory()\n");
    exit(0);
  }
  struct dirent* entry;

  while ((entry = readdir(dir)) != NULL) { // read the directory

    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue; // do nothing for these directories

    if (entry->d_type == DT_REG) { // only care about .txt files
      struct filePathBuffer f;
      strcpy(f.msgText, path);

      strcat(f.msgText, entry->d_name); // construct file path to send
      f.msgType = hashFunction(f.msgText, nReducers) + 1; // + 1 because hash function returns from 0 to nReducers - 1
      if (msgsnd(qid, (void *) &f, sizeof(struct filePathBuffer), 0) == -1) {  // send file path to reducer
        printf("failed to send message in traverseDirectory() for reducerID %ld\n", f.msgType);
        exit(0);
      }
    }
  }
  if (closedir(dir) == -1) { // close directory
    printf("failed to close directory in traverseDirectory() for mapperID %d\n", mapperID);
    exit(0);
  }
  return 0;
}

void shuffle(int nMappers, int nReducers) {
  // open message queue
  key_t key = ftok(".", 5584353); // generating a unique ke
  int qid;
  if ((qid = msgget(key, PERM | IPC_CREAT)) < 0) { // creating the message queue
    printf("failed to create message queue in shuffle()\n");
    exit(0);
  }
  // traverse the directory of each Mapper and send the word filepath to the reducers
  for(int i = 1; i <= nMappers; i++){
    traverseDirectory(i, qid, nReducers);
  }
  //send END message to reducers
  struct filePathBuffer endmsg;

  strcpy(endmsg.msgText, ENDMSG);
  for(int i = 1; i <= nReducers; i++){
    endmsg.msgType = i;
    if (msgsnd(qid, &endmsg, sizeof(struct filePathBuffer), 0) == -1) {
      printf("failed to send message in shuffle() for reducerID %d\n", i);
      exit(0);
    }
  }
  // wait for ACK from the reducers for END notification
  for(int i = 0; i < nReducers; i++){
    if (msgrcv(qid, &endmsg, sizeof(struct filePathBuffer), ACKTYPE, 0) == -1) {
      printf("failed to receive ack message in shuffle()\n");
      exit(0);
    }
  }
  // close the message queue
  msgctl(qid, IPC_RMID, 0);
}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}

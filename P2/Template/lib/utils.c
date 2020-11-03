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
  msgrcv(qid, &chunk, sizeof(struct myMsgBuffer), mapperID, 0);
  // check for END message and send ACK to master
  char* c = (char *)(malloc(sizeof(char)*(chunkSize+1)));
  strcpy(c, chunk.msgText);
  if(!strcmp(c, ENDMSG)){
    struct myMsgBuffer endmsg;
    chunk.msgType = ACKTYPE;
    strcpy(endmsg.msgText, ACKMSG);
    msgsnd(qid, &endmsg, sizeof(struct myMsgBuffer), 0);
  }
  return c;
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
  printf("in send chunk\n");
  key_t key = ftok(".", 5584353); // generating a unique key
  int qid;
  if ((qid = msgget(key, PERM | IPC_CREAT)) < 0) {
    printf("msgget error\n");
  } // creating the message queue

  struct myMsgBuffer chunk; // the buffer used to send chunks of data to the mappers
  int fd = open(inputFile, O_CREAT | O_RDONLY, 0777); // open the file
  
  // keeps track of which mapper to send data to in RR fashion
  int onMapper = 1;

  int nread;
  memset(chunk.msgText, '\0', chunkSize+1);
  while((nread = read(fd, chunk.msgText, chunkSize)) > 0){ // send messages to queue in RR fashion
    printf("buffer: %s\nnread: %d\n", chunk.msgText, nread);
    if(nread == chunkSize){ // find the closest space if nread == 1024
      int i = 0;
      while(chunk.msgText[nread+i-1] != ' '){
      
	chunk.msgText[nread+i-1] = '\0';
	i--;
      }
      chunk.msgText[nread+i-1] = '\0';
      lseek(fd, i, SEEK_CUR); // go back to space in file and skip over it
      printf("i: %d\nseek: %d\n", i, SEEK_CUR);
    }
    chunk.msgType = onMapper; // sets msg type to current mapper
    if(msgsnd(qid, &chunk, sizeof(chunk.msgText), 0) == -1){
      perror("failed to send msg\n");
    }// sends chunk to mapper
    struct myMsgBuffer recv;
    memset(chunk.msgText, '\0', chunkSize+1);
    msgrcv(qid, &recv, sizeof(chunk.msgText), onMapper, 0);
    printf("msg received for %d: %s\n", onMapper, recv.msgText);

    // set to next mapper
    onMapper++;
    if(onMapper > nMappers){
      onMapper = 1;
    }
    memset(chunk.msgText, '\0', chunkSize+1);
  }
  close(fd); // close the file as we have written out all of its contents
  printf("out of while loop\n");

  // Send END messages to each mapper
  memset(chunk.msgText, '\0', chunkSize+1);
  strcpy(chunk.msgText, ENDMSG);
  for(int i = 1; i <= nMappers; i++){
    printf("msg end i: %d\n", i);
    chunk.msgType = i;
    if(msgsnd(qid, &chunk, sizeof(struct myMsgBuffer), 0) == -1){
      perror("Failed to send end msg\n");
    }
  }
  printf("end msgs sent\n");
  // wait for ACK from mappers
  for(int i = 0; i < nMappers; i++){
    msgrcv(qid, &chunk, sizeof(struct myMsgBuffer), ACKTYPE, 0); 
  }
  printf("received acks!\n");
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
  int qid = msgget(k, PERM | IPC_CREAT); // creating the message queue
  // receive chunk from the master
  struct filePathBuffer f;
  f.msgType = reducerID;
  memset(f.msgText, '\0', filePathMsgSize+1);
  msgrcv(qid, &f, sizeof(struct filePathBuffer), reducerID, 0);
  // check for END message and send ACK to master
  //char cmp[filePathMsgSize+1];
  //strcpy(cmp, f.msgText);
  if(f.msgText == ENDMSG){ //*********** There is probably something wrong with this if statement
    struct myMsgBuffer endmsg;
    f.msgType = ACKTYPE;
    strcpy(f.msgText, ACKMSG);
    msgsnd(qid, &endmsg, sizeof(struct filePathBuffer), 0);
  }else{
    strcpy(key, f.msgText); // copy file path to the key
  }
  return 0;
}

// Goes through a directory and sends msgs to reducers about file paths to .txt files
int traverseDirectory(int mapperID, int qid, int nReducers){
  char* path = "output/MapOut/Map_";
  char id[5];
  sprintf(id, "%d", mapperID);
  strcat(path, id);

  DIR* dir = opendir(path);
  if(dir==NULL){
    printf("The path passed is invalid");
    return -1;
  }
  struct dirent* entry;

  while ((entry = readdir(dir)) != NULL) {

    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) 
      continue; // do nothing for these directories

    if (entry->d_type == DT_REG) { // only care about .txt files
      struct filePathBuffer f;
      strcpy(f.msgText, path);
      strcat(f.msgText, entry->d_name); // construct file path to send
      f.msgType = hashFunction(f.msgText, nReducers);
      msgsnd(qid, (void *) &f, sizeof(struct filePathBuffer), 0); // send file path to reducer
    }       
  }
  closedir(dir);
  return 0;
}

void shuffle(int nMappers, int nReducers) {
  // open message queue
  key_t key = ftok(".", 5584353); // generating a unique key
  int qid = msgget(key, PERM | IPC_CREAT); // creating the message queue
  // traverse the directory of each Mapper and send the word filepath to the reducers
  for(int i = 1; i <= nMappers; i++){
    traverseDirectory(i, qid, nReducers);
  }
  //send END message to reducers
  struct filePathBuffer endmsg;
  endmsg.msgType = ENDTYPE;
  strcpy(endmsg.msgText, ENDMSG);
  for(int i = 1; i <= nReducers; i++){
    msgsnd(qid, &endmsg, sizeof(struct filePathBuffer), 0);
  }
  // wait for ACK from the reducers for END notification
  for(int i = 0; i < nReducers; i++){
    msgrcv(qid, &endmsg, sizeof(struct filePathBuffer), ACKTYPE, 0); 
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

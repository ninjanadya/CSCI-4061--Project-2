## Information

* The purpose of this program is to count each unique word in a text file.

* Compile the program using the make command as follows:
$ make

Then run:
$ ./mapreduce #mappers #reducers inputFile

Where 
 #mappers is the number of mapper processes
 #reducers is the number of reducer processes
 inputFile is the textFile to test

Example:
$ ./mapreduce 5 2 test/T1/F1.txt

* The program accepts arguments consisting of the number of mapper and reducer processes, and an input test file in which the program will count the number of unique words. The program calls mapper and creates text files for each individual and unique word, in which it prints a number of 1's representing an instance of the word from the inputFile. Once mappers have finished spawning, reducer accepts the text files that were created by mapper, and compiles them into a single file per number of reducer processes inputted earlier. These files includes each unique word and the count of appearances of the word found in the inputFile.

* Assumptions were made solely based on the ones in the Project 1 and 2 documents.

#Contributions
* Team met via Zoom and worked on the code via a shared github repository
* Majority of coding, testing, and debugging were performed by Maxim and Sean
* README.md writen by Nadya

* test machine : apollo.cselabs.umn.edu
* date : 11-03-2020
* name : Maxim Zabilo, Sean Berg, Nadya Postolaki
* x500 : zabil003, berg2007, posto018

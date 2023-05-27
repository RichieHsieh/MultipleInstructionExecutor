#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<fcntl.h>
#include<unistd.h>
#include<signal.h>
#include<sys/types.h>
#include<sys/wait.h>
#include<sys/stat.h>

#include "libParseArgs.h"
#include "libProcessControl.h"

/**
 * parallelDo -n NUM -o OUTPUT_DIR COMMAND_TEMPLATE ::: [ ARGUMENT_LIST ...]
 * build and execute shell command lines in parallel
 */

/**
 * create and return a newly malloced command from commandTemplate and argument
 * the new command replaces each occurrance of {} in commandTemplate with argument
 */
char *createCommand(char *commandTemplate, char *argument){
	char *command=NULL;
	// YOUR CODE GOES HERE
	
	// Set up parameters
	int argLen = strlen(argument);
	int commandLen = strlen(commandTemplate);
	
	int count1 = 0; // Loop over the commandTemplate by count1.
	int count2 = 0; // Use count2 to count the total number of {} pair exist in the commandTemplate.
	
	while (count1 < commandLen){
		if (commandTemplate[count1] == '{'){
			if ((count1+1)<commandLen && commandTemplate[count1+1] == '}') {
				count2++;
				count1++;
			}
		}
		count1++;
	}
	
	// Malloc enough spaces for new command.
	command = malloc(((count2*argLen)+commandLen)*sizeof(char));
	if (command == NULL) return NULL;
		
	count1 = 0; 		// Loop over the commandTemplate by count1.
	int count3 = 0; 	// Use count3 to track the number of characters copied to the new command.
	while (count1 < commandLen){
		// Speical case, handdle with the {}.
		if (commandTemplate[count1] == '{'){
			if ((count1+1)<commandLen && commandTemplate[count1+1] == '}') {
				strcat(command, argument);
				count1 += 2;
				count3 += argLen;
			}
			else{
				command[count3] = commandTemplate[count1]; 
				count1++;
				count3++;
			}
		}
		// Regular case, simply copy the character.
		else{	
			command[count3] = commandTemplate[count1];  
			count1++;
			count3++;
		}
	}
	// return the new command.
	return command;
}

typedef struct PROCESS_STRUCT {
	int pid;
	int ifExited;
	int exitStatus;
	int status;
	char *command;
} PROCESS_STRUCT;

typedef struct PROCESS_CONTROL {
	int numProcesses;
	int numRunning; 
	int maxNumRunning;
	int numCompleted;
	PROCESS_STRUCT *process;
} PROCESS_CONTROL;

PROCESS_CONTROL processControl;

void printSummary(){
	printf("%d %d %d\n", processControl.numProcesses, processControl.numCompleted, processControl.numRunning);
}
void printSummaryFull(){
	printSummary();
	int i=0, numPrinted=0;
	while(numPrinted<processControl.numCompleted && i<processControl.numProcesses){
		if(processControl.process[i].ifExited){
			printf("%d %d %d %s\n", 
					processControl.process[i].pid,
					processControl.process[i].ifExited,
					processControl.process[i].exitStatus,
					processControl.process[i].command);
			numPrinted++;
		}
		i++;
	}
}
/**
 * find the record for pid and update it based on status
 * status has information encoded in it, you will have to extract it
 */
void updateStatus(int pid, int status){

	// YOUR CODE GOES HERE
	
	// Loop over all the process and find out the target with corresoponding pid.
	// Update the status of the target process.
	int count = 0;  
	while (count < processControl.numProcesses){
		if (processControl.process[count].pid == pid){
			processControl.process[count].status = status;
			processControl.process[count].exitStatus = WEXITSTATUS(status);
			processControl.process[count].ifExited = WIFEXITED(status);
			break;
		}
		count++;
	}
	
}

void handler(int signum){

	// YOUR CODE GOES HEREd\n", process
	
	// Handle signal. (SIGUSR1 and SIGUSR2 only)
	if (signum == SIGUSR1) printSummary();
	if (signum == SIGUSR2) printSummaryFull();

}


/**
 * This function does the bulk of the work for parallelDo. This is called
 * after understanding the command line arguments. runParallel 
 * uses pparams to generate the commands (createCommand), 
 * forking, redirecting stdout and stderr, waiting for children, ...
 * Instead of passing around variables, we make use of globals pparams and
 * processControl. 
 
 */
int runParallel(){

	// YOUR CODE GOES HERE
	// THERE IS A LOT TO DO HERE!!
	// TAKE SMALL STEPS, MAKE SURE THINGS WORK AND THEN MOVE FORWARD.
	
	// Set signal handdler.
	signal(SIGUSR1, handler);
	signal(SIGUSR2, handler);
	
	// Set up the attributes of PROCESS_CONTROL with the attributes of pparams.
	processControl.numProcesses = pparams.argumentListLen;
	processControl.numRunning = 0;
	
	if (pparams.maxNumRunning <= 0) {
		printf("ERROR: The maximum number of running processes is less than 1.\n");
		exit(1); // Error if the maxNumRunning <= 0.
	}
	processControl.maxNumRunning = pparams.maxNumRunning;
	processControl.numCompleted = 0;
	
	// Malloc enough spaces for all the processes.
	processControl.process = malloc(processControl.numProcesses*sizeof(PROCESS_STRUCT));
	if (processControl.process == NULL) {
		return 1;
	}
	
	// Create a new directory with pparams.outputDir.
	pid_t mkdir_pid = fork();
	if (mkdir_pid == 0){
		execl("/bin/mkdir", "mkdir", pparams.outputDir, (char *) NULL);
		perror("ERROR");
		exit(1);
	}
	else if (mkdir_pid > 0){
		int mkstatus;
		wait(&mkstatus);
	}
	else {
		perror("ERROR");
		exit(1);
	}
	
	// Before parallel executing every processes, 
	// loop over all the processes and create runable commands.
	// Then generate new PROCESS_STRUCTs to store the information about the processes.
	int count = 0;
	while (count < processControl.numProcesses) {
		processControl.process[count].ifExited = 0;
		processControl.process[count].exitStatus = 0;
		processControl.process[count].status = 0;
		processControl.process[count].command = createCommand(pparams.commandTemplate, pparams.argumentList[count]);
		
		count++;
	}
	
	// Parallel execute all the commands, loop over the processes stored in the processControl.
	int created = 0;
	while (processControl.numCompleted < processControl.numProcesses){
		
		// Create new child while numRunning is less then maxNumRunning.
		if (processControl.numRunning < processControl.maxNumRunning && created < processControl.numProcesses) {
			processControl.numRunning++;
			pid_t pid = fork();
			if (pid == 0){  // Child goes here, execute the command.
				
				// Create new PID.stdout and PID.stderr to store the result of executing the command.
				// Compute the name of the return file first.
				char *prefix = strcat(pparams.outputDir, "/");
				char *prefix2 = strdup(prefix);
				
				char *pidString = malloc(10*sizeof(char)); 
				sprintf(pidString, "%d", getpid());
				char *pidString2 = strdup(pidString);
				
				char *format = strcat(pidString, ".stdout");
				format = strcat(prefix, format);
				char *format2 = strcat(pidString2, ".stderr");
				format2 = strcat(prefix2, format2);
				
				// Create PID.stdout
				int fdout;
				if ((fdout=open(format, O_WRONLY|O_CREAT|O_TRUNC, 0666)) < 0) {
					perror("outfile");
					return(1);
				} 
				
				// Create PID.stderr
				int fderr;
				if ((fderr=open(format2, O_WRONLY|O_CREAT|O_TRUNC, 0666)) < 0) {
					perror("errfile");
					return(1);
				} 
				
				// Redirect the stdout and stderr to the file, rather than the console.
				dup2(fdout, 1);
				dup2(fderr,2);
				
				close(fderr); 
				close(fdout);
				
				// Execute the command.
				execlp("bash", "bash", "-c", processControl.process[created].command, (char *)NULL);
				perror("ERROR");
				exit(1);
			}
			else{	// Parent goes here, update the child pid for the process.
				processControl.process[created].pid = pid;
				created++;
			}
		}
		else{	// Parent side, wait for all the children and update their status after they finish their work.
			int child_status;
			pid_t child_pid;
			child_pid = wait(&child_status);
			
			// Update the status of the child.
			updateStatus(child_pid, child_status);
			processControl.numCompleted += 1; // After update, the process finish.
			
			processControl.numRunning -= 1;  // We can create new child to run more command.
		}
	}
	printSummaryFull();  // Print summary.
	
	// Free all the malloc spaces.
	int freed = 0;
	while (freed < processControl.numProcesses){
		free(processControl.process[freed].command);
		freed++;
	}
	free(processControl.process);
	
	return 0;
}

#define _GNU_SOURCE
#include <fcntl.h> 
#include <sys/types.h>
#include<sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include<wait.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include "sbuffer.h"			//shared memory (lab9)
#include "sensor_db.h"	//lab7  SQL
#include "connmgr.h" 		//lab8  TCP
#include "datamgr.h"		//lab6
#include "config.h"			
#include "errmacros.h"
#define MAX 1024
typedef struct	sharedata {
		int server_port ;
		sbuffer_t  *buffer;
		int	sequence_num;
		//void (*fifo_write)();
}sharedata_t;	



/*
//shared buffer
sbuffer_t  *buffer=NULL;
*/
//mutex
pthread_mutex_t data_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t fifo_mutex = PTHREAD_MUTEX_INITIALIZER;

void *connmgr(void *arg )
 {
	 	sharedata_t* shared = (sharedata_t*)arg;
		connmgr_listen(shared->server_port, &(shared->buffer),&shared->sequence_num, &data_mutex,&fifo_mutex);
		connmgr_free();
		pthread_exit( NULL );
 }

void *sensor_db(void *arg  )
 {
		char *send_buf ; 
		FILE		*fp;
		int presult;	
		presult = mkfifo(FIFO_NAME, 0666);			  // Create the FIFO if it does not exist 
		CHECK_MKFIFO(presult);
		
		sharedata_t* shared = (sharedata_t*)arg;
		DBCONN * db=init_connection(1,&shared->sequence_num,&fifo_mutex);
		storagemgr_parse_sensor_data(db, &(shared->buffer),&shared->sequence_num,&data_mutex,&fifo_mutex);
		
		//fifo_lock
		presult = pthread_mutex_lock( &fifo_mutex );
		ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 	
		
		disconnect(db);////////////////////////////////////////disconnect  function
		fp = fopen(FIFO_NAME, "w"); 
		FILE_OPEN_ERROR(fp);
		
		asprintf(&send_buf, "%d %lu Connection to SQL server lost", shared->sequence_num,(time_t )time(NULL));
		//printf("%s\n",send_buf);
		ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
		FFLUSH_ERROR(fflush(fp));
		free(send_buf);
		shared->sequence_num++;
				
		presult = fclose( fp );
		FILE_CLOSE_ERROR(presult);
		presult = pthread_mutex_unlock( &fifo_mutex );
		ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n"); 	
		//fifo unlock	
		pthread_exit( NULL );
 }

void *datamgr( void *arg )
 {
		FILE	*	fp_sensor_map=NULL;
		sharedata_t* shared = (sharedata_t*)arg;
		datamgr_parse_sensor_data(fp_sensor_map, &(shared->buffer),&shared->sequence_num,&data_mutex,&fifo_mutex);
		datamgr_free();
		pthread_exit( NULL );
 }

int main(int argc, char *argv[])
{
		
		int 			result;
		 int 	pfds[2];
		 result = pipe2( pfds ,O_NONBLOCK);
		SYSCALL_ERROR( result );
		//int	sequence_num=0;
		pid_t		child_pid; 
		sharedata_t	shared;
		shared.buffer=NULL;
		shared.sequence_num=0;
		
		//int 			server_port ;
		if (argc != 2)
		{
				printf("Use this program with 2 command line options: \n");
				printf("\t%-15s : TCP server port number\n", "\'server port\'");
				exit(1);
		}
		else
				shared.server_port = atoi(argv[1]);
		
		child_pid = fork();
		SYSCALL_ERROR(child_pid);
		if(child_pid == 0)
		{
			/*
			what i have to do is received data from FIFO and put it to a log file called “ gateway.log ”
			*/
			//run child process
			close(pfds[1]);
			FILE *fp;
			char * str_result;
			char recv_buf[MAX],done[MAX];
			//int a;
			result = mkfifo(FIFO_NAME, 0666);			  // Create the FIFO if it does not exist 
			CHECK_MKFIFO(result);
			fp = fopen(FIFO_NAME, "r"); 
			FILE_OPEN_ERROR(fp);
			fclose(fopen("gateway.log","w"));

			while(1)
			{
				//printf("fuck\n");
				str_result=fgets(recv_buf,MAX,fp);
				if(str_result != NULL)
				{
					printf("received= %s\n",recv_buf);
					FILE *log_fp; 
                    log_fp = fopen("gateway.log","a");
					FILE_OPEN_ERROR(log_fp);
                    //fprintf(log_fp,"%s \n",recv_buf);
					ERROR_HANDLER(fputs(recv_buf,log_fp) == EOF,"error writing data to log \n"); 	
					ERROR_HANDLER(fputs("\n",log_fp) == EOF,"error writing data to log \n"); 	
					result =fclose(log_fp);
					FILE_CLOSE_ERROR(result);
                }
				result = read(pfds[0], done, MAX);
				//SYSCALL_ERROR( result );
				//if(strncmp(recv_buf, "done", 4)==0)
				if(result>0)
				{
					printf("received= %s\n",done);
					close( pfds[0] );
					break;
				}
			}	
			
			result =fclose(fp);
			FILE_CLOSE_ERROR(result);
			printf("Child process  is terminating ...\n");
			exit(5);
		}
		else
		{			
				
				pthread_t  thread_con, thread_datamgr,thread_db;
				//BUFFER INIT
				ERROR_HANDLER(sbuffer_init(&shared.buffer)==SBUFFER_FAILURE,"SBUFFER FAILURE\n"); 
				//create connmgr thread
				result = pthread_create( &thread_con, NULL, &connmgr, &shared);
				ERROR_HANDLER(result!=0,"pthread_create fail\n"); 
				//create datamgr thread
				result = pthread_create( &thread_datamgr, NULL, &datamgr, &shared);
				ERROR_HANDLER(result!=0,"pthread_create fail\n"); 
				//create sensor_db thread
				result = pthread_create( &thread_db, NULL, &sensor_db, &shared);
				ERROR_HANDLER(result!=0,"pthread_create fail\n"); 
				
				// important: don't forget to join, otherwise main thread exists and destroys the mutex
				result= pthread_join(thread_con, NULL);
				ERROR_HANDLER(result!=0,"pthread_join fail\n"); 
		
				result= pthread_join(thread_datamgr, NULL);
				ERROR_HANDLER(result!=0,"pthread_join fail\n"); 
				
				result= pthread_join(thread_db, NULL);
				ERROR_HANDLER(result!=0,"pthread_join fail\n"); 
				sbuffer_free(&shared.buffer);

				char *send_buf ; 
				close(pfds[0]); 
				
				asprintf(&send_buf, "done");
				//printf("%s\n",send_buf);
				write(pfds[1], send_buf, strlen(send_buf)+1 ); // don't forget to send \0!
				free(send_buf);
				close(pfds[1]);

				  // waiting on 1 child
				printf("wait for child terminate\n");
				waitpid(child_pid, NULL, 0);

				result = pthread_mutex_destroy( &data_mutex );
				ERROR_HANDLER(result!=0,"pthread_mutex_destroy fail\n"); 
				result = pthread_mutex_destroy( &fifo_mutex );
				ERROR_HANDLER(result!=0,"pthread_mutex_destroy fail\n"); 
				printf("Parent process  is terminating ...\n");
				pthread_exit(NULL);
		}
		exit(EXIT_SUCCESS);
}
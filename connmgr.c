#define _GNU_SOURCE
#include <inttypes.h>
#include <memory.h>
#include <netinet/in.h>
#include <sys/types.h>
#include<sys/stat.h>
#include "errmacros.h"
#include "connmgr.h"
typedef struct {
	sensor_data_t	sensor_data;
	tcpsock_t 	*client;
	int	c_sd;		//socket descriptor	
	int	connect_flag;
} data_in_node;


dplist_t * list = NULL;
tcpsock_t *server;

void * element_copy(void *element);
void element_free(void **element){}
int element_compare(void *x, void *y);


void connmgr_listen(int port_number, sbuffer_t ** buffer,int *seq_num,pthread_mutex_t *data_mutex,pthread_mutex_t* fifo_mutex){
	
	char *send_buf ; 
	FILE		*fp;
	int presult;	
	presult = mkfifo(FIFO_NAME, 0666);			  // Create the FIFO if it does not exist 
	CHECK_MKFIFO(presult);
	
	
	int bytes,result[3];
	struct	pollfd	*poll_list;
	int	serversd;
	nfds_t nfds=1;    //for amount element in poll_list
	list = dpl_create(&element_copy, &element_free, &element_compare);
	
	if(tcp_passive_open(&server,port_number)!=TCP_NO_ERROR)	exit(EXIT_FAILURE);
	tcp_get_sd(server, &serversd);
	poll_list = malloc(sizeof(struct pollfd));
	poll_list[0].fd 			= serversd;
	poll_list[0].events	= POLLIN;
	
	//printf("count=%d\n",conn_counter);
	
	while(1)
	{
		int rv = poll(poll_list,nfds,TIMEOUT*1000);
		if(rv<=0)
		{	
			//fifo_lock
			presult = pthread_mutex_lock( fifo_mutex );
			ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 		
			for(int temp=0;temp<dpl_size(list);temp++)
			{
						ERROR_HANDLER(tcp_close( &(((data_in_node *)dpl_get_element_at_index(list,temp))->client))!=TCP_NO_ERROR,"TCP close error\n"); 
						fp = fopen(FIFO_NAME, "w"); 
						FILE_OPEN_ERROR(fp);					 
						
						asprintf(&send_buf, "%d %lu The sensor node with ID= %hd has closed the connection", *seq_num,(time_t )time(NULL),((data_in_node *)dpl_get_element_at_index(list,temp))->sensor_data.id);
						//printf("%s\n",send_buf);
						ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
						FFLUSH_ERROR(fflush(fp));
						free(send_buf);
						(*seq_num)++;
													
						presult = fclose( fp );
						FILE_CLOSE_ERROR(presult);
				
			}	
			presult = pthread_mutex_unlock( fifo_mutex );
			ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n"); 	
			//fifo unlock
			
			free(poll_list);
			break;
		}
		else
		{				
			//if Listening socket is readable
			if(poll_list[0].revents & POLLIN)
			{
				//add client
				data_in_node * dummy = malloc(sizeof(data_in_node));
				ERROR_HANDLER(tcp_wait_for_connection(server,&dummy->client)!=TCP_NO_ERROR,"TCP connect error\n"); 
				//printf("connection build\n");
				
				tcp_get_sd(dummy->client, &(dummy->c_sd));
				time(&dummy->sensor_data.ts);//initialize
				dummy->connect_flag=0;
				list = dpl_insert_at_index(list, dummy, nfds,true);//always insert at last
				struct	pollfd	*temp;
				
				temp = realloc(poll_list,sizeof(struct pollfd)*(nfds+1));
				poll_list=temp;
				
				poll_list[nfds].fd= dummy->c_sd;
				poll_list[nfds].events = POLLIN;
				nfds++;
				dummy->client=NULL;
				free(dummy);
				//continue;
			}
			
			for(int i=1;i<nfds;i++)
			{
				//read sensor data
				data_in_node * dummy = (data_in_node *)dpl_get_element_at_index(list,i-1);
				if(poll_list[i].revents & POLLIN)
				{
					bytes = sizeof(dummy->sensor_data.id);
					result [0]= tcp_receive( dummy->client,(void *)&(dummy->sensor_data.id),&bytes); 
					bytes = sizeof(dummy->sensor_data.value);  
					result [1]= tcp_receive( dummy->client,(void *)&(dummy->sensor_data.value),&bytes);
					bytes = sizeof(dummy->sensor_data.ts);
					result [2]= tcp_receive( dummy->client,(void *)&(dummy->sensor_data.ts),&bytes);
					if( result[0]==TCP_NO_ERROR && dummy->connect_flag==0)
					{
						//fifo_lock
						presult = pthread_mutex_lock( fifo_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 				

						fp = fopen(FIFO_NAME, "w"); 
						FILE_OPEN_ERROR(fp);
						
						asprintf(&send_buf, "%d %lu A sensor node with ID=%" PRIu16 " has opened a new connection", *seq_num,(time_t )time(NULL),dummy->sensor_data.id);
						//printf("%s\n",send_buf);
						ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
						FFLUSH_ERROR(fflush(fp));
						free(send_buf);
						(*seq_num)++;
						dummy->connect_flag=1;

						presult = fclose( fp );
						FILE_CLOSE_ERROR(presult);
						presult = pthread_mutex_unlock( fifo_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n"); 	
						//fifo unlock
					}
					if ((result[0]==TCP_NO_ERROR) &&(result[2]==TCP_NO_ERROR)&&(result[1]==TCP_NO_ERROR))
					{
						//lock
						presult = pthread_mutex_lock( data_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 
						
						ERROR_HANDLER(sbuffer_insert(*buffer,&(dummy->sensor_data))==SBUFFER_FAILURE,"SBUFFER FAILURE\n"); 
						
						//unlock
						presult = pthread_mutex_unlock( data_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n"); 
						
						//printf("sensor id = %" PRIu16 " - temperature = %g - timestamp = %ld\n", dummy->sensor_data.id, dummy->sensor_data.value, (long int)dummy->sensor_data.ts);
					}
					else
					{							
							//fifo_lock
							presult = pthread_mutex_lock( fifo_mutex );
							ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 				
							ERROR_HANDLER(tcp_close( &dummy->client )!=TCP_NO_ERROR,"TCP close error\n");
							//printf("The sensor node with ID=%" PRIu16 " has closed the connection\n",dummy->sensor_data.id);		
							fp = fopen(FIFO_NAME, "w"); 
							FILE_OPEN_ERROR(fp);					 
							
							asprintf(&send_buf, "%d %lu The sensor node with ID=%" PRIu16 " has closed the connection", *seq_num,(time_t )time(NULL),dummy->sensor_data.id);
							//printf("%s\n",send_buf);
							ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
							FFLUSH_ERROR(fflush(fp));
							free(send_buf);
							(*seq_num)++;
														

							presult = fclose( fp );
							FILE_CLOSE_ERROR(presult);
							presult = pthread_mutex_unlock( fifo_mutex );
							ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n");
							//fifo unlock
							dpl_remove_at_index(list,i-1,false);
							for(int temp=i;temp<nfds-1;temp++)
								poll_list[temp]=poll_list[temp+1];
						  nfds--;
					}
					dummy=NULL;
					continue;
				}
				else
				{	
					//data_in_node * dummy = (data_in_node*)get_element_with_pollist(poll_list[i].fd);
					time_t current_time;
					time(&current_time);
					if(current_time-(dummy->sensor_data.ts)>(TIMEOUT))
					{
						//printf("\ncurrent time =%ld\n",current_time);
						//printf("\nlast  connect  time =%ld\n",dummy->sensor_data.ts);
						//fifo_lock
						presult = pthread_mutex_lock( fifo_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 			
						
						ERROR_HANDLER(tcp_close( &dummy->client )!=TCP_NO_ERROR,"TCP close error\n"); 
						fp = fopen(FIFO_NAME, "w"); 
						FILE_OPEN_ERROR(fp);					 
						
						asprintf(&send_buf, "%d %lu The sensor node with ID=%" PRIu16 " has closed the connection", *seq_num,(time_t )time(NULL),dummy->sensor_data.id);
						//printf("%s\n",send_buf);
						ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
						FFLUSH_ERROR(fflush(fp));
						free(send_buf);
						(*seq_num)++;
													
						presult = fclose( fp );
						FILE_CLOSE_ERROR(presult);
						presult = pthread_mutex_unlock( fifo_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n");
						//fifo unlock
						dummy=NULL;
						dpl_remove_at_index(list,i-1,false);
							for(int temp=i;temp<nfds-1;temp++)
								poll_list[temp]=poll_list[temp+1];
						  nfds--;
					}
				}
			}
		}
	}
}
void connmgr_free()
{
	dpl_free(&list,false);
	tcp_close(&server);
}
void * (element_copy)(void * src_element)
{
	data_in_node* temp;
	temp=malloc(sizeof(data_in_node));
	*temp=*((data_in_node*)(src_element));
	
	return temp;
}

int (element_compare)(void * x, void * y)
{

	if(((data_in_node*)x)->c_sd==((data_in_node*)y)->c_sd)	
		return 0;
	else 
		return 1;
}
///////this  is test main
/*
int main(int argc, char *argv[])
{

// argv[1] = server port
 
	int server_port;
	//int i, bytes,sleep_time;
	if (argc != 2)
	{
		printf("asdda\n");
	}
	else
	{
    // to do: user input validation!
		server_port = atoi(argv[1]);
		////printf("%d\n",server_port);
	}
	
	connmgr_listen(server_port );
	connmgr_free();
	return 0;
}*/
//command
//./a.out 5678 &
//./client 102 2 127.3.0.1 5678 &
//./client 101 2 127.0.0.1 5678 &
//./client 103 2 126.0.0.1 5678 &
#define _GNU_SOURCE
#include <assert.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/types.h>
#include<sys/stat.h>
#include "errmacros.h"
#include "datamgr.h"
#ifndef		TIMEOUT
#define	TIMEOUT 10
#endif

#define FILE_ERROR(fp,error_msg) 	do { \
					  if ((fp)==NULL) { \
					    printf("%s\n",(error_msg)); \
					    exit(EXIT_FAILURE); \
					  }	\
					} while(0)
#define DATAMGR_FLAG	1		
dplist_t * sensor_list = NULL;
void*	(d_element_copy)(void * src_element);			  
void	(d_element_free)(void ** element);
int	(d_element_compare)(void * x, void * y);

typedef struct {
	sensor_data_t	sensor_data;
	uint16_t room_id;
	sensor_value_t temp[RUN_AVG_LENGTH];
	int flag;
} data_in_node;

/*
 * Reads continiously all data from the shared buffer data structure, parse the room_id's
 * and calculate the running avarage for all sensor ids
 * When *buffer becomes NULL the method finishes. This method will NOT automatically free all used memory
 */
void datamgr_parse_sensor_data(FILE * fp_sensor_map, sbuffer_t ** buffer,int *seq_num,pthread_mutex_t *data_mutex,pthread_mutex_t* fifo_mutex)
{	
		char *send_buf ; 
		FILE		*fp;
		int	i=0,index=0,err,presult;
		presult = mkfifo(FIFO_NAME, 0666);			  // Create the FIFO if it does not exist 
		CHECK_MKFIFO(presult);
		
		fp_sensor_map = fopen("room_sensor.map", "r");
		FILE_ERROR(fp_sensor_map,"Couldn't create room_sensor.map\n");
		sensor_list = dpl_create(* (d_element_copy), (d_element_free),(d_element_compare));
		data_in_node *data = malloc( sizeof(data_in_node) );
		data->flag=0;
		time_t current_time, last_connect;
	
		while(fscanf(fp_sensor_map, "%" SCNd16 "%" SCNd16, &(data->room_id), &(data->sensor_data.id)) != EOF)
		{
			dpl_insert_at_index(sensor_list,data,i++,1);
		}
		i=0;
		fclose(fp_sensor_map);
		
		//lock
		presult = pthread_mutex_lock( data_mutex );
		ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n");
		
		err=sbuffer_remove(*buffer,&(data->sensor_data),DATAMGR_FLAG);
		ERROR_HANDLER(err==SBUFFER_FAILURE,"SBUFFER FAILURE\n");
		time(&last_connect);
		
		while(1)
		{
				if(err==SBUFFER_SUCCESS)
				{
					//unlock
					presult = pthread_mutex_unlock( data_mutex );
					ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n");
					
					double count=0;
					//first check if sensor exists in the sensor_list and get its index
					for(index=0;index<dpl_size(sensor_list);index++)
					{
						if(((data_in_node *)dpl_get_element_at_index(sensor_list,index))->sensor_data.id== data->sensor_data.id)
							break;
						
						//fifo_lock
						presult = pthread_mutex_lock( fifo_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 		
						fp = fopen(FIFO_NAME, "w"); 
						FILE_OPEN_ERROR(fp);
						//Sensor id does not exist
						if(index==dpl_size(sensor_list)-1)
						{	
							asprintf(&send_buf, "%d %lu Received sensor data with invalid sensor node ID= %hd", *seq_num,(time_t )time(NULL),data->sensor_data.id);
							//printf("%s\n",send_buf);
							ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
							FFLUSH_ERROR(fflush(fp));
							free(send_buf);
							(*seq_num)++;
						}
						
						presult = fclose( fp );
						FILE_CLOSE_ERROR(presult);
						presult = pthread_mutex_unlock( fifo_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n"); 	
						//fifo unlock						
					}
					((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.ts=data->sensor_data.ts;
					for(int j=0;j<RUN_AVG_LENGTH;j++)
						count+=((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[j];
					if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->flag==RUN_AVG_LENGTH-1)
						count=count-((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[0];
					for(int j=0;j<RUN_AVG_LENGTH-1;j++)//left shift
						((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[j]=((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[j+1];
					//fill the last element 
					((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[RUN_AVG_LENGTH-1]=data->sensor_data.value;
					count=count+data->sensor_data.value;
					((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value=count/RUN_AVG_LENGTH;
					
					//printf("in datamgr %f\n",((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value);
					
					if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->flag<RUN_AVG_LENGTH-1)
						((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->flag++;
					else 
					{
						//fifo_lock
						presult = pthread_mutex_lock( fifo_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 		
						fp = fopen(FIFO_NAME, "w"); 
						FILE_OPEN_ERROR(fp);
						if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value>SET_MAX_TEMP)
						{
							asprintf(&send_buf, "%d %lu The sensor node with ID= %hd reports it’s too HOT, and avg temperature= %f", *seq_num,(time_t )time(NULL),((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.id, ((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value);
							//printf("%s\n",send_buf);
							ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
							FFLUSH_ERROR(fflush(fp));
							free(send_buf);
							(*seq_num)++;
						}
						if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value<SET_MIN_TEMP)
						{//fprintf(stderr, "room %"PRIu16" is too cold at %ld\n",((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->room_id,((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.ts);	
							asprintf(&send_buf, "%d %lu The sensor node with ID= %hd reports it’s too COLD, and avg temperature= %f", *seq_num,(time_t )time(NULL),((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.id, ((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value);
							//printf("%s\n",send_buf);
							ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
							FFLUSH_ERROR(fflush(fp));
							free(send_buf);
							(*seq_num)++;	
						}
						presult = fclose( fp );
						FILE_CLOSE_ERROR(presult);
						presult = pthread_mutex_unlock( fifo_mutex );
						ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n"); 	
						//fifo unlock
					}
					time(&last_connect);
				}
				if(err==SBUFFER_NO_DATA)
				{
					time(&current_time);
					if(current_time-last_connect>(TIMEOUT))
					{	
							//unlock
							presult = pthread_mutex_unlock( data_mutex );
							ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n");
							break;
					}
					//unlock
					presult = pthread_mutex_unlock( data_mutex );
					ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n");
					//finishes
					//usleep(50);
				}
				
				//lock
				presult = pthread_mutex_lock( data_mutex );
				ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n");
				//even the sbuffer is empty, still do this until unless timeout happens
				err=sbuffer_remove(*buffer,&(data->sensor_data),DATAMGR_FLAG);
				ERROR_HANDLER(err==SBUFFER_FAILURE,"SBUFFER FAILURE\n");		
		}
}	
	
	
   
void datamgr_parse_sensor_files(FILE * fp_sensor_map, FILE * fp_sensor_data)
{
	uint16_t	room,sensor;
	sensor_value_t	temperature;
	sensor_ts_t	time;
	int	i=0,index=0;
	fp_sensor_map = fopen("room_sensor.map", "r");
	FILE_ERROR(fp_sensor_map,"Couldn't create room_sensor.map\n");
	sensor_list = dpl_create(* (d_element_copy), (d_element_free),(d_element_compare));
	data_in_node *data = malloc( sizeof(data_in_node) );
	data->flag=0;

	while(fscanf(fp_sensor_map, "%" SCNd16 "%" SCNd16, &room, &sensor) != EOF)
	{
		data->sensor_data.id = sensor;
		data->room_id = room;
  		dpl_insert_at_index(sensor_list,data,i,1);
		i++;
	}
	free(data);
	data=NULL;
	i=0;
	//int k=0;
	fclose(fp_sensor_map);

		fp_sensor_data = fopen("sensor_data", "r");
		fread(&sensor, sizeof(sensor), 1, fp_sensor_data );

		fread(&temperature, sizeof(temperature), 1, fp_sensor_data );

		fread(&time, sizeof(time_t), 1, fp_sensor_data );   

		while(!feof(fp_sensor_data ))
		{
				double count=0;
				for(index=0;index<dpl_size(sensor_list);index++)
				{
					if(sensor==((data_in_node*)dpl_get_element_at_index( sensor_list, index))->sensor_data.id)
						break;
					if(index==dpl_size(sensor_list)-1)
						fprintf(stderr, "this sensor id does not exist\n");	
				}
				((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.ts=time;
				for(int j=0;j<RUN_AVG_LENGTH;j++)
					count+=((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[j];
				if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->flag==RUN_AVG_LENGTH)
					count=count-((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[0];
				for(int j=0;j<RUN_AVG_LENGTH-1;j++)//left shift
					((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[j]=((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[j+1];
				//fill the last element 
				((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->temp[RUN_AVG_LENGTH-1]=temperature;
				count=count+temperature;
				((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value=count/RUN_AVG_LENGTH;
				if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->flag<RUN_AVG_LENGTH)
					((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->flag++;
				else 
				{
					if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value>SET_MAX_TEMP)
						fprintf(stderr, "room %"PRIu16" is too hot at %ld\n",((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->room_id,((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.ts);
					if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value<SET_MIN_TEMP)
						fprintf(stderr, "room %"PRIu16" is too cold at %ld\n",((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->room_id,((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.ts);	
				}
			
			fread(&sensor, sizeof(sensor), 1, fp_sensor_data );
			fread(&temperature, sizeof(temperature), 1, fp_sensor_data );
				//if(index==6&&sensor==132)  printf("temp  = %f\n",temperature);
			fread(&time, sizeof(time_t), 1, fp_sensor_data );  
		}
		
		fclose(fp_sensor_data);

	
}

void datamgr_free()
{
	dpl_free(&sensor_list, false);
}

uint16_t datamgr_get_room_id(sensor_id_t sensor_id)
{
	int index=0;
	while(index<dpl_size(sensor_list))
	{
		if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.id==sensor_id)
			return ((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->room_id;
		else index++;
	}	
	//printf("this sensor id does not exist\n");
	fprintf(stderr, "this sensor id does not exist\n");
	exit(0);
}

sensor_value_t datamgr_get_avg(sensor_id_t sensor_id)
{
	int index=0;
	while(index<dpl_size(sensor_list))
	{
		if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.id==sensor_id)
			return ((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.value;
		else index++;
	}	
	//printf("this sensor id does not exist\n");
	fprintf(stderr, "this sensor id does not exist\n");
	exit(0);
}

time_t datamgr_get_last_modified(sensor_id_t sensor_id)
{
	int index=0;
	while(index<dpl_size(sensor_list))
	{
		if(((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.id==sensor_id)
			return ((data_in_node*)dpl_get_element_at_index( sensor_list, index ))->sensor_data.ts;
		else index++;
	}	
	//printf("this sensor id does not exist\n");
	fprintf(stderr, "this sensor id does not exist\n");
	exit(0);
}



int datamgr_get_total_sensors()
{
	return dpl_size( sensor_list );
}




void * (d_element_copy)(void * src_element)
{
	data_in_node* temp;
	temp=malloc(sizeof(data_in_node));
	*temp=*((data_in_node*)(src_element));
	return temp;
}

void (d_element_free)(void ** element)
{
	free(*element);
	*element=NULL;
}

int (d_element_compare)(void * x, void * y)
{
	if((*(char*)(x))==(*(char*)(y)))
		return 0;
	else 
		return 1;
}

#define _GNU_SOURCE	
#include <sys/types.h>
#include<sys/stat.h>
#include	"sensor_db.h"
#include	<inttypes.h>
#include	<string.h>
#include <unistd.h>
#include "errmacros.h"
#define DB_FLAG	2
#ifndef		DB_TIMEOUT
#define	DB_TIMEOUT 15
#endif

void storagemgr_parse_sensor_data(DBCONN * conn, sbuffer_t ** buffer,int *seq_num,pthread_mutex_t *data_mutex,pthread_mutex_t* fifo_mutex)
{
/*
 * Reads continiously all data from the shared buffer data structure and stores this into the database
 * When *buffer becomes NULL the method finishes. This method will NOT automatically disconnect from the db
 */
	sensor_data_t data;
	int 	err,presult;
	time_t current_time, last_connect;
	time(&last_connect);
	//lock
	presult = pthread_mutex_lock( data_mutex );
	ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 
	
	err=sbuffer_remove(*buffer,&data,DB_FLAG);
	ERROR_HANDLER(err==SBUFFER_FAILURE,"SBUFFER FAILURE\n");

	while(1)
	{
		if(err==SBUFFER_SUCCESS)
		{	
				ERROR_HANDLER(insert_sensor(conn, data.id, data.value, data.ts),"rc != SQLITE_OK \n");
				time(&last_connect);
				//unlock
				presult = pthread_mutex_unlock( data_mutex );
				ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n"); 
				//usleep(50);
				
				//lock
				presult = pthread_mutex_lock( data_mutex );
				ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 
				
				err=sbuffer_remove(*buffer,&data,DB_FLAG);
				ERROR_HANDLER(err==SBUFFER_FAILURE,"SBUFFER FAILURE\n");

		}	
		if(err==SBUFFER_NO_DATA)
		{
				time(&current_time);
				if(current_time-last_connect >(DB_TIMEOUT))
				{
					//unlock
					presult = pthread_mutex_unlock( data_mutex );
					ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n");
					break;
				}
				//unlock
				presult = pthread_mutex_unlock( data_mutex );
				ERROR_HANDLER(presult!=0,"pthread_mutex_unlock  fail\n");
				//usleep(50);
				//lock
				presult = pthread_mutex_lock( data_mutex );
				ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 
				
				err=sbuffer_remove(*buffer,&data,DB_FLAG);
				ERROR_HANDLER(err==SBUFFER_FAILURE,"SBUFFER FAILURE\n");
				//finishes
		}
	}	
}


DBCONN * init_connection(char clear_up_flag,int *seq_num,pthread_mutex_t* fifo_mutex)
{
	char *send_buf ; 
	FILE		*fp;
	int presult;	
	DBCONN * db;
	
	char *err_msg = 0;
	int con_attempt=0;
	int	rc,temp;
	/*error detect*/
	//fifo_lock
	presult = pthread_mutex_lock( fifo_mutex );
	ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 		
	fp = fopen(FIFO_NAME, "w"); 
	FILE_OPEN_ERROR(fp);
	
	for(con_attempt=0;con_attempt<3;con_attempt++)
	{
		rc	=	sqlite3_open(TO_STRING(DB_NAME), &db);
		if (rc == SQLITE_OK) 
			break;		
	}
	if(con_attempt==3)
	{
		//fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
		asprintf(&send_buf, "%d %lu Unable to connect to SQL server", *seq_num,(time_t )time(NULL));
		//printf("%s\n",send_buf);
		ERROR_HANDLER(fputs(send_buf,fp) == EOF,"error writing data to fifo \n"); 	
		FFLUSH_ERROR(fflush(fp));
		free(send_buf);
		(*seq_num)++;
		sqlite3_close(db);
		presult = fclose( fp );
		return NULL;
	}
	else
	{
		asprintf(&send_buf, "%d %lu Connection to SQL server established", *seq_num,(time_t )time(NULL));
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
	/*---------------*/
	
	char *sql=malloc(sizeof(char)*300);
	
	snprintf(sql,300,"SELECT "TO_STRING(TABLE_NAME)"FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='dbName'");
	temp= sqlite3_exec(db, sql, NULL, 0, &err_msg);
	
	
	snprintf ( sql, 300, "CREATE "TO_STRING(TABLE)" IF NOT EXISTS "TO_STRING(TABLE_NAME)" (ID INTEGER PRIMARY KEY AUTOINCREMENT, sensor_id INT , sensor_value DECIMAL(4,2) , timestamp TIMESTAMP );");
	rc = sqlite3_exec(db, sql, NULL, 0, &err_msg);
	
	//fifo_lock
	presult = pthread_mutex_lock( fifo_mutex );
	ERROR_HANDLER(presult!=0,"pthread_mutex_lock  fail\n"); 		
	fp = fopen(FIFO_NAME, "w"); 
	FILE_OPEN_ERROR(fp);
	if(temp==1 &&rc==SQLITE_OK)
	{
		asprintf(&send_buf, "%d %lu New table \""TO_STRING(TABLE_NAME)"\" created", *seq_num,(time_t )time(NULL));
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
	
	/*error detect*/ 
	 if (rc != SQLITE_OK )
	{   
        fprintf(stderr, "SQL error: %s\n", err_msg);   
        sqlite3_free(err_msg);        
        sqlite3_close(db);
        exit(0);
    }

	/*---------------*/
	if(clear_up_flag==1)
	{
		sql=TO_STRING(DELETE FROM TABLE_NAME;);
		rc = sqlite3_exec(db, sql, NULL, 0, &err_msg);
		if (rc != SQLITE_OK )
		{   
			fprintf(stderr, "SQL error: %s\n", err_msg);   
			sqlite3_free(err_msg);        
			sqlite3_close(db);
			exit(0);
		} 
	}
	return db;
}

void disconnect(DBCONN *conn)
{
	
	sqlite3_close(conn);
}

int insert_sensor(DBCONN * conn, sensor_id_t s_id, sensor_value_t value, sensor_ts_t ts)
{
	char *err_msg = 0,*sql=malloc(sizeof(char)*300);
	snprintf ( sql, 300, "INSERT INTO  "TO_STRING(TABLE_NAME) "(sensor_id, sensor_value,timestamp)VALUES (%hd, %f, %ld);",s_id, value, ts);
	//printf("%s\n",sql);
	int	rc= sqlite3_exec(conn, sql, NULL, 0, &err_msg);
	/*error detect*/ 
	 if (rc != SQLITE_OK )
	{   
        fprintf(stderr, "SQL error: %s\n", err_msg);   
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return 1;
    } 
	/*---------------*/
	return 0;
}

int insert_sensor_from_file(DBCONN * conn, FILE * sensor_data)
{
	uint16_t	sensor;
	sensor_value_t	temperature;
	sensor_ts_t	time;
	sensor_data = fopen("sensor_data", "r");
	fread(&sensor, sizeof(sensor), 1, sensor_data );
	fread(&temperature, sizeof(temperature), 1, sensor_data );
	fread(&time, sizeof(time_t), 1, sensor_data ); 
	while(!feof(sensor_data ))
	{
		if(insert_sensor(conn, sensor, temperature, time))
		{       
			//sqlite3_close(conn);
			fclose(sensor_data);
			return 1;
		} 
		fread(&sensor, sizeof(sensor), 1, sensor_data );
		fread(&temperature, sizeof(temperature), 1, sensor_data );
		fread(&time, sizeof(time_t), 1, sensor_data ); 
	}	
	fclose(sensor_data);
	return 0;
}

int find_sensor_all(DBCONN * conn, callback_t f)
{
	char *err_msg = 0, *sql=malloc(sizeof(char)*300);
	snprintf ( sql, 300, "SELECT * FROM " TO_STRING(TABLE_NAME)";");
	int	rc= sqlite3_exec(conn, sql, f, 0, &err_msg);
	 if (rc != SQLITE_OK )
	{   
        fprintf(stderr, "SQL error: %s\n", err_msg);   
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return 1;
    } 
	return 0;
}

int find_sensor_by_value(DBCONN * conn, sensor_value_t value, callback_t f)
{
	char *err_msg = 0, *sql=malloc(sizeof(char)*300);
	snprintf ( sql, 300, "SELECT * FROM " TO_STRING(TABLE_NAME)" WHERE  sensor_value = %f;",value);
	int	rc= sqlite3_exec(conn, sql, f, 0, &err_msg);
	 if (rc != SQLITE_OK )
	{   
        fprintf(stderr, "SQL error: %s\n", err_msg);   
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return 1;
    } 
	return 0;
	
}

int find_sensor_exceed_value(DBCONN * conn, sensor_value_t value, callback_t f)
{
	char *err_msg = 0, *sql=malloc(sizeof(char)*300);
	snprintf ( sql, 300, "SELECT * FROM " TO_STRING(TABLE_NAME)" WHERE  sensor_value > %f;",value);
	int	rc= sqlite3_exec(conn, sql, f, 0, &err_msg);
	 if (rc != SQLITE_OK )
	{   
        fprintf(stderr, "SQL error: %s\n", err_msg);   
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return 1;
    } 
	return 0;
}

int find_sensor_by_timestamp(DBCONN * conn, sensor_ts_t ts, callback_t f)
{
	char *err_msg = 0, *sql=malloc(sizeof(char)*300);
	snprintf ( sql, 300, "SELECT * FROM " TO_STRING(TABLE_NAME)" WHERE  timestamp = %ld;",ts);
	int	rc= sqlite3_exec(conn, sql, f, 0, &err_msg);
	 if (rc != SQLITE_OK )
	{   
        fprintf(stderr, "SQL error: %s\n", err_msg);   
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return 1;
    } 
	return 0;
}

int find_sensor_after_timestamp(DBCONN * conn, sensor_ts_t ts, callback_t f)
{
	char *err_msg = 0, *sql=malloc(sizeof(char)*300);
	snprintf ( sql, 300, "SELECT * FROM " TO_STRING(TABLE_NAME)" WHERE  timestamp > %ld;",ts);
	int	rc= sqlite3_exec(conn, sql, f, 0, &err_msg);
	 if (rc != SQLITE_OK )
	{   
        fprintf(stderr, "SQL error: %s\n", err_msg);   
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return 1;
    } 
	return 0;
}

#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <stdint.h>
#include <time.h>

#define FIFO_NAME 	"logFifo"
#define ERROR_HANDLER(condition,...) 	\
	do { 						\
		if (condition)	\
		{ 					\
			printf("\nError: in %s - function %s at line %d: %s\n", __FILE__, __func__, __LINE__, __VA_ARGS__ );  	\
			exit(EXIT_FAILURE); \
		 }	\
	} while(0)
//for event
#define			NEW_CONNECTION				0
#define			CONNECTION_CLOSED		1

typedef uint16_t sensor_id_t;
typedef double sensor_value_t;     
typedef time_t sensor_ts_t;         // UTC timestamp as returned by time() - notice that the size of time_t is different on 32/64 bit machine

typedef struct{
	sensor_id_t id;
	sensor_value_t value;
	sensor_ts_t ts;
} sensor_data_t;
			

#endif /* _CONFIG_H_ */


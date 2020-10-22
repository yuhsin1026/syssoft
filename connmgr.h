#ifndef CONNMGR_H
#define CONNMGR_H
#include "./lib/tcpsock.h"
#include "./lib/dplist.h"
#include <stdlib.h>
#include <stdio.h>
#include "sbuffer.h"
#include <poll.h>
#include <limits.h>
#include <pthread.h>
#ifndef	TIMEOUT
#define	TIMEOUT 5
#endif
/*
 * This method starts listening on the given port and when when a sensor node connects it 
 * stores the sensor data in the shared buffer.
 */
void connmgr_listen(int port_number, sbuffer_t ** buffer,int *seq_num,pthread_mutex_t *data_mutex,pthread_mutex_t* fifo_mutex);

/*
 * This method should be called to clean up the connmgr, and to free all used memory. 
 * After this no new connections will be accepted
 */
void connmgr_free();
void * get_element_with_pollist(int fd);

#endif /* CONNMGR_H */


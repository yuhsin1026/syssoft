#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include "sbuffer.h"


/*
 * All data that can be stored in the sbuffer should be encapsulated in a
 * structure, this structure can then also hold extra info needed for your implementation
 */
struct sbuffer_data {
		sensor_data_t data;
		int buffer_flag;
};

typedef struct sbuffer_node {
		struct sbuffer_node * next;
		sbuffer_data_t element;
} sbuffer_node_t;

struct sbuffer {
		sbuffer_node_t * head;
		sbuffer_node_t * tail;
};	


int sbuffer_init(sbuffer_t ** buffer)
{
  *buffer = malloc(sizeof(sbuffer_t));
  if (*buffer == NULL) return SBUFFER_FAILURE;
  (*buffer)->head = NULL;
  (*buffer)->tail = NULL;
  return SBUFFER_SUCCESS; 
}


int sbuffer_free(sbuffer_t ** buffer)
{
  sbuffer_node_t * dummy;
  if ((buffer==NULL) || (*buffer==NULL)) 
  {
    return SBUFFER_FAILURE;
  } 
  while ( (*buffer)->head )
  {
    dummy = (*buffer)->head;
    (*buffer)->head = (*buffer)->head->next;
    free(dummy);
  }
  free(*buffer);
  *buffer = NULL;
  return SBUFFER_SUCCESS;		
}


int sbuffer_remove(sbuffer_t * buffer,sensor_data_t * data,int i)
{
		sbuffer_node_t * dummy;
		if (buffer == NULL) return SBUFFER_FAILURE;
		if (buffer->head == NULL) return SBUFFER_NO_DATA;
		//i meet this data twice
		if(i==buffer->head->element.buffer_flag)
		{
			// buffer has only one node
			if(buffer->head==buffer->tail) return SBUFFER_NO_DATA;
			else 
			{
				for(dummy = buffer->head;dummy->next!=NULL;dummy=dummy->next)
					if(dummy->next->element.buffer_flag==0)
					{	
						*data = dummy->next->element.data;
						dummy->next->element.buffer_flag=i;
						return SBUFFER_SUCCESS;
					}
				return SBUFFER_NO_DATA;
			}
		}	
		//i==0 or someone else use this already 
		//if(i!=buffer->head->element.flag)
		else
		{
				*data = buffer->head->element.data;		
				//second time
				if(buffer->head->element.buffer_flag!=0)
				{
						dummy = buffer->head;
						if (buffer->head == buffer->tail) // buffer has only one node
						{
							buffer->head = buffer->tail = NULL; 
						}
						else  // buffer has many nodes empty
						{
							buffer->head = buffer->head->next;
						}
						free(dummy);
				}
				//first time
				else 
						buffer->head->element.buffer_flag=i;
		}
		return SBUFFER_SUCCESS;
}


int sbuffer_insert(sbuffer_t * buffer, sensor_data_t * data)
{
  sbuffer_node_t * dummy;
  if (buffer == NULL) return SBUFFER_FAILURE;
  dummy = malloc(sizeof(sbuffer_node_t));
  if (dummy == NULL) return SBUFFER_FAILURE;
  dummy->element.data = *data;
  dummy->element.buffer_flag=0;
  dummy->next = NULL;
  if (buffer->tail == NULL) // buffer empty (buffer->head should also be NULL
  {
    buffer->head = buffer->tail = dummy;
  } 
  else // buffer not empty
  {
    buffer->tail->next = dummy;
    buffer->tail = buffer->tail->next; 
  }
  return SBUFFER_SUCCESS;
}




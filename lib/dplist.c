#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "dplist.h"

/*
 * definition of error codes
 * */
#define DPLIST_NO_ERROR 0
#define DPLIST_MEMORY_ERROR 1 // error due to mem alloc failure
#define DPLIST_INVALID_ERROR 2 //error due to a list operation applied on a NULL list 

#ifdef DEBUG
	#define DEBUG_PRINTF(...) 									         \
		do {											         \
			fprintf(stderr,"\nIn %s - function %s at line %d: ", __FILE__, __func__, __LINE__);	 \
			fprintf(stderr,__VA_ARGS__);								 \
			fflush(stderr);                                                                          \
                } while(0)
#else
	#define DEBUG_PRINTF(...) (void)0
#endif


#define DPLIST_ERR_HANDLER(condition,err_code)\
	do {						            \
            if ((condition)) DEBUG_PRINTF(#condition " failed\n");    \
            assert(!(condition));                                    \
        } while(0)

        
/*
 * The real definition of struct list / struct node
 */

struct dplist_node {
  dplist_node_t * prev, * next;
  void * element;
};

struct dplist {
  dplist_node_t * head;
  void * (*element_copy)(void * src_element);			  
  void (*element_free)(void ** element);
  int (*element_compare)(void * x, void * y);
};


dplist_t * dpl_create (// callback functions
			  void * (*element_copy)(void * src_element),
			  void (*element_free)(void ** element),
			  int (*element_compare)(void * x, void * y)
			  )
{
  dplist_t * list;
  list = malloc(sizeof(struct dplist));
  //DPLIST_ERR_HANDLER(list==NULL,DPLIST_MEMORY_ERROR);
  list->head = NULL;  
  list->element_copy = element_copy;
  list->element_free = element_free;
  list->element_compare = element_compare; 
  return list;
}

void dpl_free(dplist_t ** list, bool free_element)
{
    // add your code here
	while( (dpl_size(*list)) != 0)
		dpl_remove_at_index(*list,(dpl_size(*list)-1),free_element);
	free(*list);
	//free(list);
	*list=NULL;
}

dplist_t * dpl_insert_at_index(dplist_t * list, void * element, int index, bool insert_copy)
{
    // add your code here
	dplist_node_t * ref_at_index, * list_node;
	//DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
	list_node = malloc(sizeof(dplist_node_t));
	//DPLIST_ERR_HANDLER(list_node==NULL,DPLIST_MEMORY_ERROR);
	if(insert_copy)
		list_node->element = list->element_copy(element);

	else 
		(list_node->element) = element;
	
		
	// pointer drawing breakpoint
	if (list->head == NULL)  
	{ // covers case 1 
		list_node->prev = NULL;
		list_node->next = NULL;
		list->head = list_node;
	// pointer drawing breakpoint
	} else if (index <= 0)  
		{ // covers case 2 
			list_node->prev = NULL;
			list_node->next = list->head;
			list->head->prev = list_node;
			list->head = list_node;
		// pointer drawing breakpoint
		} else 
		{
			ref_at_index = dpl_get_reference_at_index(list, index);  
			assert( ref_at_index != NULL);
			// pointer drawing breakpoint
			if (index < dpl_size(list))
			{ // covers case 4
			list_node->prev = ref_at_index->prev;
			list_node->next = ref_at_index;
			ref_at_index->prev->next = list_node;
			ref_at_index->prev = list_node;
			// pointer drawing breakpoint
			} 
			else
			{ // covers case 3 
			assert(ref_at_index->next == NULL);
			list_node->next = NULL;
			list_node->prev = ref_at_index;
			ref_at_index->next = list_node;    
			// pointer drawing breakpoint
			}
		}
	return list;
}

dplist_t * dpl_remove_at_index( dplist_t * list, int index, bool free_element)
{
    // add your code here
	dplist_node_t * dummy=dpl_get_reference_at_index( list,index);	
	//assert( dummy != NULL);
	//DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
	//printf("%d\n",*(int*)dummy->element);
	//printf("%d\n",dpl_get_index_of_element(list,dummy->element));
	if (list->head == NULL ) return list;
	if(free_element)
		list->element_free(&(dummy->element));
	else
	{	
		free(dummy->element);
		dummy->element=NULL;
	}
	//printf("%d\n",dpl_get_index_of_element(list,dummy->element));
	if (list->head->next==NULL)
	{
		free(list->head);
		list->head=NULL;
		return list;
	}
	else if (index<=0)
	{
		dummy->next->prev=NULL;
		list->head=dummy->next;
		free(dummy);
		return list;
	}

	if(dummy->next==NULL)
	{	
		//dummy=dpl_get_reference_at_index(list,index);
		//dummy->next=NULL;
		dummy->prev->next=NULL;
		free(dummy);
		return list;
	}

	//dummy=dpl_get_reference_at_index(list,index);
	dummy->next->prev=dummy->prev;
	dummy->prev->next=dummy->next;
	free(dummy);
	return list;
}

int dpl_size( dplist_t * list )
{
    // add your code here
	int count=1;
	dplist_node_t * dummy=list->head;
	if (list->head == NULL)  return 0;
	while(dummy->next != NULL) 
	{ 
		dummy = dummy->next; 
		count++; 
	}
	return count;
}

void * dpl_get_element_at_index( dplist_t * list, int index )
{
    // add your code here
	int count;
	dplist_node_t * dummy;
	//DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
	if (list->head == NULL ) 	return (void *)0;
	for ( dummy = list->head, count = 0; dummy->next != NULL ; dummy = dummy->next, count++) 
	{
		if (count >= index) return (dummy->element);
	}  
	return (dummy->element);  
}

int dpl_get_index_of_element( dplist_t * list, void * element )
{
    // add your code here
	int count=0;
	dplist_node_t * dummy;
	//DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
	if (list->head == NULL)	return -1;
	for ( dummy = list->head; dummy->next != NULL ; dummy = dummy->next) 
	{
		if (list->element_compare(dummy->element,element)==0) return count;
		count++;
	}  
	if(list->element_compare(dummy->element,element)==0)
		return count;
	else 
		return -1;
}

dplist_node_t * dpl_get_reference_at_index( dplist_t * list, int index )
{
    // add your code here
	int count;
	dplist_node_t * dummy;
	//DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
	if (list->head == NULL ) return NULL;
	for ( dummy = list->head, count = 0; dummy->next != NULL ; dummy = dummy->next, count++) 
	{	
		if (count >= index) return dummy;  
	}
	return dummy;  
}
 
void * dpl_get_element_at_reference( dplist_t * list, dplist_node_t * reference )
{
    // add your code here
	dplist_node_t * dummy=list->head;
	//DPLIST_ERR_HANDLER(list==NULL,DPLIST_INVALID_ERROR);
	if	(list->head == NULL)	return NULL;
	//for ( dummy = list->head; dummy->next != NULL ; dummy = dummy->next) 
	do
	{	
		if (dummy == reference || (reference == NULL && dummy->next== NULL)) return dummy->element;
		dummy = dummy->next;
	}while(dummy!=NULL);
	//return list;
	return NULL;
}



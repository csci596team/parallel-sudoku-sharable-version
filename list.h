#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <omp.h>


typedef struct Node 
{
	// if num == -1, the Node is dummy
	int num;  
	int pos;
	struct Node* next;
	struct Node* prev;
}Node;


typedef struct List
{
	struct Node* head;
	struct Node* tail;
	omp_lock_t lock;
}List;


Node* new_node(int n, int p);

List* init_list();
void clear_list(List* list);

void insert_head(List* list, Node* node);
void insert_tail(List* list, Node* node);

Node* pop_head(List* list);
Node* pop_tail(List* list);

bool is_empty(List* list);
int get_list_size(List* list);






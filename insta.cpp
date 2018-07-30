#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <malloc.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <dos.h>
#include <Windows.h>

int row_index = 0;
int threadPool[5] = { 0 };

pthread_mutex_t lock;
pthread_t tid[5];
pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;

struct Column
{
	char* value;
	int version;
	struct Column *next_version;
};

struct Row
{
	int rno;
	int current_version;
	int no_of_cols;
	Column* columns;
};

struct Row* table = (struct Row*)malloc(sizeof(struct Row) * 100);
struct Row* posttable = (struct Row*)malloc(sizeof(struct Row) * 100);
int rno = 0, rno1 = 0;
int no_of_cols = 4;
char values[4] = { '1', '2', '3', '4' };


int get_index_of_row(int rno, struct Row* table)
{
	for (int i = 0; i < row_index; i++)
	{
		if (table[i].rno == rno)
		{
			return i;
		}
	}
	return -1;
}

void intialize_posttable(int row_index,int rno, struct Row *table)
{
	table[row_index].rno = rno;
	table[row_index].columns = (struct Column*)malloc(sizeof(struct Column) * 20);
	table[row_index].current_version = 1;
	table[row_index].no_of_cols = 0;
}

//to insert a complete row
void put(int rno, int no_of_cols, char* values[], struct Row* table,struct Row* posttable)
{
	table[row_index].rno = rno;
	table[row_index].current_version = 1;
	table[row_index].columns = (struct Column*)malloc(sizeof(struct Column)*no_of_cols + 1);
	for (int i = 0; i < no_of_cols; i++)
	{
		table[row_index].columns[i].value = (char*)malloc(sizeof(char) * 30);
		strcpy(table[row_index].columns[i].value, values[i]);
		table[row_index].columns[i].next_version = NULL;
		table[row_index].columns[i].version = 1;
	}
	table[row_index].no_of_cols = no_of_cols;
	intialize_posttable(row_index, rno, posttable);
	row_index++;	
}

void* putpost(void *args)
{
	
	printf("\nstarted writing into file");
	/*table[row_index].rno = rno;
	table[row_index].current_version = 1;
	table[row_index].columns = (struct Column*)malloc(sizeof(struct Column)*no_of_cols + 1);
	for (int i = 0; i < no_of_cols; i++)
	{
		table[row_index].columns[i].value = (char*)malloc(sizeof(char) * 30);
		strcpy(table[row_index].columns[i].value, values);
		table[row_index].columns[i].next_version = NULL;
		table[row_index].columns[i].version = 1;
	}
	table[row_index].no_of_cols = no_of_cols;
	intialize_posttable(row_index, rno, posttable);
	row_index++;
	rno++;*/
	int p = (int)args;
	threadPool[p] = 0;
	printf("\ncompleted writing into file");
	return 0;
}

//inserting a single col
void put_column(int rno, char* value, struct Row* table,struct Row* usertable)
{
	int i;
	i = get_index_of_row(rno, usertable);
	int no_of_cols = table[i].no_of_cols;
	table[i].columns[no_of_cols].value = (char*)malloc(sizeof(char) * 30);
	strcpy(table[i].columns[no_of_cols].value, value);
	table[i].columns[no_of_cols].next_version = NULL;
	table[i].columns[no_of_cols].version = 1;
	table[i].no_of_cols = no_of_cols + 1;
}

//to update a column in a table
void update(int rno, int col, char* value, struct Row* table)
{
	struct Column* temp = (struct Column*)malloc(sizeof(struct Column) * 10);
	temp->value = value;
	temp->next_version = NULL;
	for (int i = 0; i < row_index; i++)
	{
		if (table[i].rno == rno)
		{
			rno = i;
			break;
		}
	}
	if (table[rno].current_version < temp->version)
		table[rno].current_version = temp->version;
	while (table[rno].columns[col].next_version != NULL)
	{
		table[rno].columns[col] = *table[rno].columns[col].next_version;
	}
	table[rno].columns[col].next_version = temp;
	temp->version = table[rno].columns[col].version + 1;
}


//delete a row
void delete_row(int rno, struct Row* table)
{
	int index = get_index_of_row(rno, table);
	free(table[index].columns);
	table[index].rno = -1;
}

//printing the latest version
void print_latest(int i, struct Row* table)
{
	int j;
	if (table[i].rno == -1)
		return;
	printf("\n%d\t", table[i].rno);
	for (j = 0; j < table[i].no_of_cols; j++)
	{
		while (table[i].columns[j].next_version != NULL)
			table[i].columns[j] = *table[i].columns[j].next_version;
		printf("%s\t", table[i].columns[j]);
	} 
}

//to print a particular row
void get(int rno, struct Row* table)
{
	int index = get_index_of_row(rno, table);
	print_latest(index, table);
}

void* getpost(void* args)
{
	//FILE *fp;
	//fp = fopen("client.txt", "a+");
	printf("\nstarted reading file");
	/*int i = get_index_of_row(rno1, table);
	int j;
	if (table[i].rno == -1)
		return 0;
	fprintf(fp,"\n%d\t", table[i].rno);
	for (j = 0; j < table[i].no_of_cols; j++)
	{
		fprintf(fp,"%s\t", table[i].columns[j]);
	}
	fclose(fp);*/
	printf("\nfinished reading file");
	rno1++;
	return 0;
}

//to print all rows
void print_all(struct Row* table)
{
	int i;
	for (i = 0; i < row_index; i++)
	{
		print_latest(i, table);
	}
}


//to get the latest feed of a user
void load_feed(struct Row* user_table, struct Row* post_table, struct Row* feed_table)
{
	int i,j,l=0,no_of_followers,k=0;
	int follow;
	char* follower=(char*)malloc(sizeof(char)*40);
	for (i = 0; i < row_index; i++)
	{
		feed_table[i].rno = user_table[i].rno;
		no_of_followers = user_table[i].no_of_cols;
		feed_table[i].columns = (struct Column*)malloc(sizeof(struct Column) * 20);
		l = 0;
		feed_table[i].no_of_cols = 0;
		feed_table[i].current_version = 1;
		for (j = 0; j < no_of_followers; j++)
		{
			strcpy(follower,user_table[i].columns[j].value);
			follow = atoi(follower);
			follow = get_index_of_row(follow,user_table);
			for (k = 0; k < post_table[follow].no_of_cols; k++)
			{
				while (post_table[follow].columns[k].next_version != NULL)
					post_table[follow].columns[k] = *post_table[follow].columns[k].next_version;
				feed_table[i].columns[l].value = (char*)malloc(sizeof(char) * 50);
				strcpy(feed_table[i].columns[l].value, post_table[follow].columns[k].value);
				feed_table[i].columns[l].next_version = NULL;
				feed_table[i].columns[l].version = 1;
				l++;
				feed_table[i].no_of_cols = feed_table[i].no_of_cols + 1; 
			}
		}
	}
}

void store_tables(char* tablename)
{
	FILE *fp = fopen("metadata.txt", "a");
	fprintf(fp, "%s ", tablename);
	fprintf(fp, "%d\n", row_index);
}

void dump(struct Row* table,char* tablename)
{
	store_tables(tablename);
	char *filename1 = (char*)malloc(sizeof(char)*20);
	char *filename2 = (char*)malloc(sizeof(char) * 20);
	char *str = (char*)malloc(sizeof(char) * 50);
	strcpy(filename1, tablename);
	strcat(filename1, "_rows.txt");
	strcpy(filename2, tablename);
	strcat(filename2, "_cols.txt");
	FILE *fp1 = fopen(filename1, "w");
	FILE *fp2 = fopen(filename2, "w");
	int i,j;
	for (i = 0; i < row_index; i++)
	{
		fprintf(fp1,"%d ",table[i].rno);
		fprintf(fp1, "%d ", table[i].current_version);
		fprintf(fp1, "%d\n", table[i].no_of_cols);
		for (j = 0; j < table[i].no_of_cols; j++)
		{
			while (table[i].columns[j].next_version != NULL)
				table[i].columns[j] = *table[i].columns[j].next_version;
			fprintf(fp2, "%s ", table[i].columns[j].value);
			fprintf(fp2, "%d ", table[i].columns[j].version);
		}
		fputs("\n", fp2);
	}
	fclose(fp1);
	fclose(fp2);
}

void get_data_from_files()
{
	FILE *fp = fopen("metadata.txt", "r");
	char* filename = (char*)malloc(sizeof(char) * 20);
	char* buf = (char*)malloc(sizeof(char) * 100);
	char* buf2 = (char*)malloc(sizeof(char) * 100);
	int row_count;
	int i = 0,j = 0;
	while(fgets(buf, 100, fp))
	{
		j = 0;
		for (i = 0; buf[i] != ' ' && buf[i] != '\n'; i++ )
		{
			filename[i] = buf[i];
		}
		filename[i] = '\0';
		i++;
		buf2[j] = buf[i];
		buf2[j + 1] = '\0';
		row_count = atoi(buf2);
		printf("%s\t", filename);
		printf("%d\n", row_count);
	}
}

int job[10] = {0};


void allocate_job(int p ,int id)
{
	if (job[id] == 1)
	{
		
		pthread_create(&tid[p], NULL, putpost, (void*)p);
	
	}
	else if (job[id] == 2)
	{
		
		pthread_create(&tid[p], NULL, getpost, (void*)p);
		
	}
	job[id] = 0;
}

void check_pool()
{
	int i,count=0;
	int id = 0;
	while (count<2)
	{
		for (i = 0; i < 5; i++)
		{
			if (threadPool[i] == 0)
			{
				threadPool[i] = 1;
				allocate_job(i,id);
				id = (id + 1) % 10;
			}
		}
		count++;
	}
}

void createjob(int i, int id)
{
	job[id] = i;
	id++;
}

void create_jobs()
{
	int p,id = 0;
	int i = 0;
	while (i<2)
	{
		createjob(1,id);
		createjob(1,id);
		createjob(2, id);
		createjob(2, id);
		createjob(1, id);
		i++;
		id++;
	}
}

void main()
{
	int op, no_of_cols, row_no;
	struct Row* user_table,*post_table,*feed_table;
	user_table = (struct Row*)malloc(sizeof(struct Row) * 10);
	post_table = (struct Row*)malloc(sizeof(struct Row) * 10);
	feed_table = (struct Row*)malloc(sizeof(struct Row) * 10);
	char* values1[] = { "2", "3", "9", "10" };
	char* values2[] = { "1","5", "9" };
	put(1, 4, values1, user_table,post_table);
	put(2, 3, values2, user_table,post_table);
	put(3, 3, values1, user_table,post_table);
	put(5, 3, values2, user_table,post_table);
	put(9, 3, values1, user_table,post_table);
	put(10, 3, values2, user_table,post_table);
	put_column(1, "my first post", post_table,user_table);
	put_column(10, "hello!!", post_table, user_table);
	put_column(9, "i had a nice day", post_table, user_table);
	put_column(2, "good morning", post_table, user_table);
	put_column(1, "hello!!", post_table, user_table);
	load_feed(user_table, post_table, feed_table);
	print_all(user_table);
	print_all(post_table);
	printf("\n\n");
	print_all(feed_table);
}
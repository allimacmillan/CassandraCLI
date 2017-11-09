#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "config.h"
#include <cassandra.h>

char currentKeyspace[20];
char currentTable[20];

//Cassandra stuff
void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster() {
  CassCluster* cluster = cass_cluster_new();
  //cass_cluster_set_contact_points(cluster, "127.0.0.1,127.0.0.2,127.0.0.3");
  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    printf("%s\n", "connect session failed");
    print_error(future);
  }
  cass_future_free(future);

  return rc;
  printf("%s\n", "connect session done");
}

CassResult *execute_query(CassSession* session, const char *query) {

    char return_arr[100];

    CassError rc = CASS_OK;
    CassFuture* future = NULL;
    CassStatement* statement = cass_statement_new(query, 0);
    //execute
    future = cass_session_execute(session, statement);
    //free statement
    cass_statement_free(statement);
    //check future error
    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        printf("%s\n", "future error: ");
        print_error(future);
    }
    //get result
    CassResult* result = cass_future_get_result(future);
    //check result error
    if (result == NULL) {
        /* Handle error */
        printf("%s\n", "result = NULL");
        cass_future_free(future);
        return NULL;
    }
    //free future
    cass_future_free(future);
    //Iterator
    return result;
}

//end of cassandra stuff





char *readline(char *prompt);

static int tty = 0;

static void cli_about()
{
	printf("You executed a command!\n");
}

static void cli_show(CassSession *session)
{
	printf("in cli_show\n");
	const char *query = "SELECT * FROM system_schema.keyspaces;";
	CassResult *result = execute_query(session, query);
    CassIterator* row_iterator = cass_iterator_from_result(result);
    while (cass_iterator_next(row_iterator)) {
        const char *svalue;
        size_t size = sizeof(svalue);
        const CassRow* row = cass_iterator_get_row(row_iterator);
        const CassValue *value = cass_row_get_column_by_name(row, "keyspace_name");
        cass_value_get_string(value, &svalue, &size);
        printf("%s\n", svalue);
    }
    cass_iterator_free(row_iterator);
}

static void cli_list(CassSession *session)
{
	printf("in cli_list\n");
	char query[100];
	strcpy(query, "select table_name from system_schema.tables where keyspace_name = '");
	strcat(query, currentKeyspace);
	strcat(query, "';");
    CassResult *result = execute_query(session, query);
    if (cass_result_row_count(result) == 0) {
        printf("%s %s\n", "No tables in keyspace", currentKeyspace);
    }
    CassIterator* row_iterator = cass_iterator_from_result(result);
    while (cass_iterator_next(row_iterator)) {
        size_t i = 0;
        const char *svalue;
        size_t size = sizeof(svalue);
        const CassRow* row = cass_iterator_get_row(row_iterator);
        const CassValue *value = cass_row_get_column(row, i);
        cass_value_get_string(value, &svalue, &size);
        printf("%s %s\n", svalue, "::");
    }
    cass_iterator_free(row_iterator);
}

static void cli_use(CassSession *session)
{
	printf("in cli_use\n");
	//check if current keyspace and current table are in database
    char query[500];
    /*size_t i = 0;
    const char *svalue;
    size_t size = sizeof(svalue);*/
    strcpy(query, "CREATE KEYSPACE IF NOT EXISTS ");
	strcat(query, currentKeyspace);
	strcat(query, " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");
	//printf("%s %s\n", "query1: ", query);
    CassResult *result = execute_query(session, query);
    //printf("%s\n", "after query1");
    //check if table exists, create it if not
    char query2[500];
    strcpy(query2, "CREATE TABLE IF NOT EXISTS ");
    strcat(query2, currentKeyspace);
    strcat(query2, ".");
    strcat(query2, currentTable);
    strcat(query2, " (key int PRIMARY KEY, value text);");
    //printf("%s %s\n", "query2: ", query2);
    CassResult *result2 = execute_query(session, query2);

}

static void cli_get()
{
	printf("in cli_get\n");
}

static void cli_insert()
{
	printf("in cli_insert\n");
}

static void cli_help()
{
	printf("in cli_help\n");
	return;
}

void cli(CassSession *session)
{
	char *cmdline = NULL;
	char cmd[BUFSIZE], prompt[BUFSIZE];
	int pos;

	tty = isatty(STDIN_FILENO);
	if (tty)
		cli_about();

	/* Main command line loop */
	for (;;) {
		if (cmdline != NULL) {
			free(cmdline);
			cmdline = NULL;
		}
		memset(prompt, 0, BUFSIZE);
		sprintf(prompt, "cassandra> ");

		if (tty)
			cmdline = readline(prompt);
		else
			cmdline = readline("");

		if (cmdline == NULL)
			continue;

		if (strlen(cmdline) == 0)
			continue;

		if (!tty)
			printf("%s\n", cmdline);

		if (strcmp(cmdline, "?") == 0) {
			cli_help();
			continue;
		}
		if (strcmp(cmdline, "quit") == 0 ||
		    strcmp(cmdline, "q") == 0)
			break;

		memset(cmd, 0, BUFSIZE);
		pos = 0;
		nextarg(cmdline, &pos, " ", cmd);

		if (strcmp(cmd, "about") == 0 || strcmp(cmd, "a") == 0) {
			cli_about();
			continue;
		}

		if (strcmp(cmd, "show") == 0) {
			cli_show(session);
			continue;
		}

		if (strcmp(cmd, "list") == 0){
			cli_list(session);
			continue;
		}

        if (strcmp(cmd, "use") == 0){
            nextarg(cmdline, &pos, " ", cmd);
            const char *dot = ".";
            const char *space = " ";
            memset(currentKeyspace, 0, strlen(currentKeyspace));
            memset(currentTable, 0, strlen(currentTable));
            char *keyspace = strtok(cmd, dot);
            //printf("%s %s\n", "keyspace: ", keyspace);
            char *table = strtok(NULL, space);
            //printf("%s %s %s\n", "table: ", table, "::");
            strcpy(currentKeyspace, keyspace);
            strcpy(currentTable, table);
            printf("%s %s %s\n", "current table: ", currentTable, "::");
			cli_use(session);
			continue;
		}

        if (strcmp(cmd, "get") == 0){
			cli_get();
			continue;
		}

        if (strcmp(cmd, "insert") == 0){
			cli_insert();
			continue;
		}
	}
}

int main(int argc, char**argv)
{
    CassCluster* cluster = create_cluster();
    CassSession* session = cass_session_new();
    CassFuture* close_future = NULL;

    if (connect_session(session, cluster) != CASS_OK) {
        cass_cluster_free(cluster);
        cass_session_free(session);
        return -1;
    }

	cli(session);

	close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);

    cass_cluster_free(cluster);
    cass_session_free(session);
	exit(0);
}

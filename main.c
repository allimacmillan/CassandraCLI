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
	char query[100];
	strcpy(query, "select * from system_schema.tables where keyspace_name = '");
	strcat(query, currentKeyspace);
	strcat(query, "';");
    CassResult *result = execute_query(session, query);
    if (result != NULL) {
        if (cass_result_row_count(result) == 0) {
            printf("%s %s\n", "No tables in keyspace", currentKeyspace);
        }
        CassIterator* row_iterator = cass_iterator_from_result(result);
        while (cass_iterator_next(row_iterator)) {
            size_t i = 0;
            const char *svalue = "";
            size_t size;
            const CassRow* row = cass_iterator_get_row(row_iterator);
            const CassValue *value = cass_row_get_column_by_name(row, "table_name");
            cass_value_get_string(value, &svalue, &size);
            printf("%s\n", svalue);

        }
        cass_iterator_free(row_iterator);
    } else {
        printf("%s\n", "please choose a keyspace");
    }
}

static void cli_use(CassSession *session)
{
    char query[500];
    strcpy(query, "CREATE KEYSPACE IF NOT EXISTS ");
	strcat(query, currentKeyspace);
	strcat(query, " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");
    CassResult *result = execute_query(session, query);

    char query2[500];
    strcpy(query2, "CREATE TABLE IF NOT EXISTS ");
    strcat(query2, currentKeyspace);
    strcat(query2, ".");
    strcat(query2, currentTable);
    strcat(query2, " (key int PRIMARY KEY, value text);");

    CassResult *result2 = execute_query(session, query2);

}

static void cli_get(CassSession *session, char *key)
{
    char query[500];
    strcpy(query, "select * from ");
    strcat(query, currentKeyspace);
    strcat(query, ".");
    strcat(query, currentTable);
    CassResult *result = execute_query(session, query);
    if (result != NULL) {
        CassIterator* row_iterator = cass_iterator_from_result(result);
        while (cass_iterator_next(row_iterator)) {
            size_t i = 0;
            const char *svalue = "";
            size_t size;
            const CassRow* row = cass_iterator_get_row(row_iterator);
            const CassValue *value = cass_row_get_column_by_name(row, key);
            CassValueType type = cass_value_type(value);
            printf("%s\n", "before checking types");
            if (type == CASS_VALUE_TYPE_INT) {
                cass_int32_t i;
                cass_value_get_int32(value, &i);
                printf("%d", i);
            } else if (type == CASS_VALUE_TYPE_BOOLEAN) {
                cass_bool_t b;
                cass_value_get_bool(value, &b);
                if (b) {
                    printf("%s\n", "True");
                } else {
                    printf("%s\n", "False");
                }
            } else if (type == CASS_VALUE_TYPE_DOUBLE) {
                cass_double_t d;
                cass_value_get_double(value, &d);
                printf("%f\n", d);
            } else if (type == CASS_VALUE_TYPE_TEXT || type == CASS_VALUE_TYPE_ASCII || type == CASS_VALUE_TYPE_VARCHAR) {
                const char *s;
                size_t size;
                cass_value_get_string(value, &s, &size);
                printf("%s\n", s);
            } else if (type == CASS_VALUE_TYPE_UUID) {
                CassUuid u;
                char s[CASS_UUID_STRING_LENGTH];
                cass_value_get_uuid(value, &u);
                cass_uuid_string(u, s);
                printf("%s\n", s);
            } else {
                if (cass_value_is_null(value)) {
                    printf("%s\n", "Null");
                } else {
                    printf("%s\n", "can not handle this type");
                }
            }
        }
        cass_iterator_free(row_iterator);
    } else {
        printf("%s\n", "please choose a keyspace and table");
    }

}

static void cli_insert(CassSession *session, char *key, char * val)
{
	printf("in cli_insert\n");
	char query[500];
	/*strcpy(query, "select type from system_schema.columns where keyspace_name = '");
	strcat(query, currentKeyspace);
	strcat(query, )
	system_auth' and table_name = 'roles' and column_name = 'role';*/
	strcpy(query, "insert into ");
	strcat(query, currentKeyspace);
	strcat(query, ".");
	strcat(query, currentTable);
	strcat(query, "(");
	strcat(query, key);
	strcat(query, ") values (");
	strcat(query, val);
	strcat(query, ");");
	CassResult *result = execute_query(session, query);

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
            char *keyspace = strtok(cmd, dot);
            //printf("%s %s\n", "keyspace: ", keyspace);
            char *table = strtok(NULL, space);
            //printf("%s %s %s\n", "table: ", table, "::");
            memset(currentKeyspace, 0, strlen(currentKeyspace));
            memset(currentTable, 0, strlen(currentTable));
            strcpy(currentKeyspace, keyspace);
            strcpy(currentTable, table);
            //printf("%s %s %s\n", "current table: ", currentTable, "::");
			cli_use(session);
			continue;
		}

        if (strcmp(cmd, "get") == 0){
            nextarg(cmdline, &pos, " ", cmd);
			cli_get(session, cmd);
			continue;
		}

        if (strcmp(cmd, "insert") == 0){
            char key[100];
            char val[100];

            nextarg(cmdline, &pos, " ", cmd);
            strcpy(key, cmd);
            nextarg(cmdline, &pos, " ", cmd);
            strcpy(val, cmd);
            printf("%s %s\n", "key: ", key);
            printf("%s %s\n", "val: ", val);

			cli_insert(session, key, val);
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

#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

int main(int argc, char **argv) {
    const char *conninfo = "host=127.0.0.1 port=5432 dbname=testdb user=postgres password=pencil";
    PGconn *conn;
    PGresult *res;
    int nFields;
    int i, j;

    if (argc > 1) {
        conninfo = argv[1];
    }

    conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }

    res = PQexec(conn, "SELECT * FROM testtable");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "SELECT failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    nFields = PQnfields(res);

    if (nFields != 5) {
        fprintf(stderr, "Expected 5 columns, got %d\n", nFields);
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    int nTuples = PQntuples(res);
    if (nTuples != 3) {
        fprintf(stderr, "Expected 3 rows, got %d\n", nTuples);
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    for (i = 0; i < nTuples; i++) {
        for (j = 0; j < nFields; j++) {
            printf("%s", PQgetvalue(res, i, j));
        }
        printf("\n");
    }

    PQclear(res);

    res = PQexec(conn, "INSERT INTO testtable VALUES (1);");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "INSERT failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    PQclear(res);

    const char *stmtName = "get_by_id";
    const char *query = "SELECT * FROM testtable WHERE id = $1";
    int nParams = 1;
    const char *paramValues[1];
    int paramLengths[1];
    int paramFormats[1];
    int resultFormat = 0;

    res = PQprepare(conn, stmtName, query, nParams, NULL);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "PREPARE failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }
    PQclear(res);

    res = PQdescribePrepared(conn, stmtName);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "DESCRIBE failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    int nFieldsPrepared = PQnfields(res);
    PQclear(res);

    char paramValue[16];
    snprintf(paramValue, sizeof(paramValue), "%d", 0);
    paramValues[0] = paramValue;
    paramLengths[0] = 0;
    paramFormats[0] = 0;

    res = PQexecPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, resultFormat);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "EXECUTE failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    int nFieldsExecuted = PQnfields(res);
    if (nFieldsExecuted != nFieldsPrepared) {
        fprintf(stderr, "Column count mismatch: describe=%d, execute=%d\n", nFieldsPrepared, nFieldsExecuted);
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    int nTuplesExecuted = PQntuples(res);
    if (nTuplesExecuted != 3) {
        fprintf(stderr, "Expected 3 rows, got %d\n", nTuplesExecuted);
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    for (i = 0; i < nTuplesExecuted; i++) {
        for (j = 0; j < nFieldsExecuted; j++) {
            printf("%s", PQgetvalue(res, i, j));
        }
        printf("\n");
    }

    PQclear(res);
    PQfinish(conn);

    return 0;
}

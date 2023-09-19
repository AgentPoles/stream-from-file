import logging
import psycopg

# Define connection to your CockroachDB cluster
def init_conn():
    db_url = 'postgresql://localhost:26257/defaultdb?sslrootcert=/Users/poamen/projects/pau/drp/assignments/stream-from-file/cockroachdb1/certs/ca.crt&sslkey=/Users/poamen/projects/pau/drp/assignments/stream-from-file/cockroachdb1/certs/client.paul.key.pk8&sslcert=/Users/poamen/projects/pau/drp/assignments/stream-from-file/cockroachdb1/certs/client.paul.crt&sslmode=verify-full&user=paul&password="@Password1"'
    conn = psycopg.connect(db_url, application_name="kaftka-cockroach illustration")
    return conn

# Get connection
def getConnection(mandatory):
    try:
        conn = init_conn()
        print("successful")
        return conn
    except Exception as e:
        logging.fatal("Database connection failed: {}".format(e))
        print("failed")
        if mandatory:
            exit(1)  # database connection must succeed to proceed.

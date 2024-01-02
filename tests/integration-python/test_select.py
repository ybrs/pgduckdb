import unittest
import psycopg2

class TestPostgreSQL(unittest.TestCase):
    def setUp(self):
        # Connect to your PostgreSQL database
        self.conn = psycopg2.connect(
            dbname="duckdb", 
            user="something", 
            password="password", 
            host="localhost",
            port=15432,
        )
        print("conn 1")
        self.conn.set_client_encoding('UTF8')
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        print("conn open, creating table")
        # Create table
        # SERIAL doesn't exist
        self.cur.execute("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR);")

    def tearDown(self):
        # Drop table and close connection
        self.cur.execute("DROP TABLE users;")
        self.cur.close()
        self.conn.close()

    def test_insert_and_query_users(self):
        users = [(1, "Alice",), (2, "Bob",), (3, "Charlie",), (4, "David",), (5, "Eve",)]
        self.cur.executemany("INSERT INTO users (id, name) VALUES (%s, %s);", users)

        # Query data
        self.cur.execute("SELECT name FROM users ORDER BY id;")
        result = self.cur.fetchall()

        # Check data
        expected = [("Alice",), ("Bob",), ("Charlie",), ("David",), ("Eve",)]
        self.assertEqual(result, expected)
        self.cur.execute("SELECT name FROM users ORDER BY id;")

    def test_named_prepared_statement(self):
        users = [(1, "Alice",), (2, "Bob",), (3, "Charlie",), (4, "David",), (5, "Eve",)]
        self.cur.executemany("INSERT INTO users (id, name) VALUES (%s, %s);", users)

        # Prepare a statement
        self.cur.execute("PREPARE my_select_plan AS SELECT name FROM users WHERE id = $1;")

        # Execute the prepared statement with different parameters
        self.cur.execute("EXECUTE my_select_plan (%s)", (1,))
        self.assertEqual("Alice", self.cur.fetchone()[0])

        self.cur.execute("EXECUTE my_select_plan (%s)", (2,))
        self.assertEqual("Bob", self.cur.fetchone()[0])

        self.cur.execute("SELECT name FROM users WHERE id = %s", (3,))
        self.assertEqual("Charlie", self.cur.fetchone()[0])


    def test_named_prepared_statement_exceptions(self):
        users = [(1, "Alice",), (2, "Bob",), (3, "Charlie",), (4, "David",), (5, "Eve",)]
        self.cur.executemany("INSERT INTO users (id, name) VALUES (%s, %s);", users)

        with self.assertRaises(psycopg2.DatabaseError):
            self.cur.execute("PREPARE my_select_plan AS SELECT namex FROM users WHERE id = $1;")

        # # Execute the prepared statement with different parameters
        with self.assertRaises(psycopg2.DatabaseError):
            self.cur.execute("EXECUTE my_select_planx (%s)", (1,))

        with self.assertRaises(psycopg2.DatabaseError):
            self.cur.execute("SELECT namex FROM users WHERE id = %s", (3,))

if __name__ == '__main__':
    unittest.main()

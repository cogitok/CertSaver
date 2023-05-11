import logging
import sys
import datetime
import certstream
import csv
import sqlite3

# Initialize global variables for the output file and database connection
OUTPUT_FILE = 'certstream_output.csv'
DB_FILE = 'certstream_output.db'
conn = sqlite3.connect(DB_FILE)

def create_table():
    """Create a table in the database to store CertStream data."""
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS certstream_data
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp TEXT,
                 domain TEXT,
                 sans TEXT)''')
    conn.commit()

def print_callback(message, context):
    """Callback function to handle incoming CertStream messages."""
    logging.debug("Message -> {}".format(message))

    if message['message_type'] == "heartbeat":
        return

    if message['message_type'] == "certificate_update":
        all_domains = message['data']['leaf_cert']['all_domains']

        if len(all_domains) == 0:
            domain = "NULL"
        else:
            domain = all_domains[0]

        # Format the data for output
        timestamp = datetime.datetime.now().strftime('%m/%d/%y %H:%M:%S')
        sans = ", ".join(message['data']['leaf_cert']['all_domains'][1:])

        # Write the data to the console
        sys.stdout.write(u"[{}] {} (SAN: {})\n".format(timestamp, domain, sans))
        sys.stdout.flush()

        # Write the data to the output file
        with open(OUTPUT_FILE, mode='a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([timestamp, domain, sans])

        # Write the data to the database
        c = conn.cursor()
        c.execute("INSERT INTO certstream_data (timestamp, domain, sans) VALUES (?, ?, ?)", (timestamp, domain, sans))
        conn.commit()

# Set up logging
logging.basicConfig(format='[%(levelname)s:%(name)s] %(asctime)s - %(message)s', level=logging.INFO)

# Create the output file and database table (if they don't exist)
with open(OUTPUT_FILE, mode='w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Timestamp', 'Domain', 'SANs'])

create_table()

# Create the CertStream listener
certstream.listen_for_events(print_callback, url='wss://certstream.calidog.io/')
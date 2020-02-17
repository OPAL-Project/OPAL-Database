"""Insert antenna records in DB."""
import configargparse
import random
import psycopg2
import os
import cPickle as pickle
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

parser = configargparse.ArgumentParser(
    description='Insert antenna records in DB.')
parser.add_argument('--host', required=True,
                    help='Host to the DB.')
parser.add_argument('--port', required=False, default=5432, type=int,
                    help='Port for the DB.')
parser.add_argument('--db', required=True,
                    help='Name of the DB.')

args = parser.parse_args()


locations = [
    'Mumbai, Maharashtra',
    'London, England',
    'Paris, France',
    'Boston, Massachusetts',
    'Delhi, Delhi',
    'Chandigarh, Punjab',
    'Brussels, Belgium',
    'Tehran, Iran',
    'Amsterdam, Netherlands',
    'Rome, Italy',
    'Zagreb, Croatia',
    'Ljubljana, Slovenia'
]


def connect():
    """Connects to the db."""
    connection = psycopg2.connect(
        database=args.db, user="postgres", password="",
        host=args.host, port=args.port)
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return connection


def get_unique_antenna_ids(connection):
    """Return distinct antenna_ids from the database."""
    cur = connection.cursor()
    cur.execute("SELECT DISTINCT antenna_id FROM public.opal;")
    result = cur.fetchall()
    cur.close()
    return result


def insert_antenna_records(connection, antenna_id):
    """Insert records for antenna to the connection specified for given antenna id."""
    num_locations = random.choice([1, 2])
    cur = connection.cursor()
    if num_locations == 1:
        location = random.choice(locations).split(', ')
        cur.execute("INSERT INTO public.antenna_records("
                    "antenna_id, date_from, date_to, latitude, longitude, "
                    "location_level_1, location_level_2) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (antenna_id, '2012-01-01', '2012-12-31 23:59:59', random.random() * 180, random.random() * 90,
                     location[0], location[1]))
    else:
        location = random.choice(locations).split(', ')
        cur.execute("INSERT INTO public.antenna_records("
                    "antenna_id, date_from, date_to, latitude, longitude, "
                    "location_level_1, location_level_2) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (antenna_id, '2012-01-01', '2012-06-30 23:59:59', random.random() * 180, random.random() * 90,
                     location[0], location[1]))
        location = random.choice(locations).split(', ')
        cur.execute("INSERT INTO public.antenna_records("
                    "antenna_id, date_from, date_to, latitude, longitude, "
                    "location_level_1, location_level_2) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (antenna_id, '2012-07-01', '2012-12-31 23:59:59', random.random() * 180, random.random() * 90,
                     location[0], location[1]))


if __name__ == "__main__":
    connection = connect()
    if not os.path.exists('antenna_ids.pkl'):
        antenna_ids = get_unique_antenna_ids(connection)
        with open('antenna_ids.pkl', 'w') as fp:
            pickle.dump(antenna_ids, fp)
    else:
        with open('antenna_ids.pkl') as fp:
            antenna_ids = pickle.load(fp)
    for antenna_id_tuple in antenna_ids:
        insert_antenna_records(connection, antenna_id_tuple[0])

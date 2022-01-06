import gzip
import os
import sys
import uuid
import re
from datetime import datetime

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')


def insert_into_db(session, table_name, data_to_insert):
    insert_logs = session.prepare(
        'INSERT INTO nasalogs (host, id, bytes, datetime, path) VALUES (?, ?, ?, ?, ?)', [table_name])
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for data in data_to_insert:
        splitted_data = line_re.split(data)
        filtered_data = [element for element in splitted_data if len(element) > 0]
        if len(filtered_data) >= 4:
            hostname = filtered_data[0]
            # '01/Jul/1995:00:00:01'
            timestamp = datetime.strptime(filtered_data[1], "%d/%b/%Y:%H:%M:%S")
            path = filtered_data[2]
            bytes_transferred = int(filtered_data[3])
            batch.add(insert_logs, (hostname, uuid.uuid1(), bytes_transferred, timestamp, path))

    session.execute(batch)


def main(input_dir, keyspace, table_name):
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)
    data_to_insert = []
    batch_size = 300
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                data_to_insert.append(line)
                if len(data_to_insert) % batch_size == 0:
                    insert_into_db(session, table_name, data_to_insert)
                    data_to_insert = []


if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_dir, keyspace, table_name)
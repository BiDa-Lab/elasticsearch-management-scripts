import csv
import requests
import time
from datetime import datetime

requested_substring = raw_input('Enter requested index pattern substring( WITHOUT *) and press enter t'
                                'for filtering): ').strip()
host = raw_input('Enter the host: ')
port = raw_input('Enter the port: ')
print

START_TIME = time.time()

csvlines = list(csv.reader(requests.get(
    'http://{}:{}/_cat/indices?v&h=status,index,store.soze.docs.count'.format(host, port)).text.splitlines(),
                           deimiter=' ',
                           skipinitialspace=True
                           )
                )

headers = csvlines[1:]
index_header_pos = headers.index('index')
status_header_pos = headers.index('status')  # whether index is close or open
store_size_header_pos = headers.index('store.size')  # the store with replica
docs_count_header_pos = headers.index('docs.count')  # without replica

indices_info = csvlines[1:]

total_size_in_gb = 0
total_doc_count = 0
total_indices_count = 0

oldest_index_name = None
oldest_index_creation_data = None
youngest_index_name = None
youngest_index_creation_date = None

matching_indices_closed = []

for index_info in indices_info:
    index_name = index_info[index_header_pos]
    store_size_as_text = index_info[store_size_header_pos]
    is_index_closed = index_info[status_header_pos].lower() == 'close'

    if requested_substring in index_name:
        if is_index_closed:
            print 'Index \'{}\' matches search, but is closed. CAn\'t work with it'.format(index_name)
            matching_indices_closed.append(index_name)
            continue  # next index

        print 'Index: {} store size: {}'.format(index_name, store_size_as_text)
        total_indices_count += 1

        docs_count = int(index_info[docs_count_header_pos])
        size_type = store_size_as_text[-2:].lower()
        store_size_as_textual_number = store_size_as_text[:-2]
        try:
            store_size_number = float(store_size_as_textual_number[:-2])
        except ValueError:
            raise ValueError('Couldn\'t convert \'{}\' to float. Index: {}'
                             .format(store_size_as_textual_number, index_name))
        if size_type == 'gb':
            store_size_in_gb = store_size_number
        elif size_type == 'mb':
            store_size_in_gb = store_size_number / 1024.0
        elif size_type == 'kb':
            store_size_in_gb = store_size_number / 1024.0 / 1024.0
        elif size_type == 'tb':
            store_size_in_gb = store_size_number * 1024.0
        elif store_size_as_text[-1] == 'b':
            store_size_in_gb = store_size_number / 1024.0 / 1024.0 / 1024.0
        else:
            raise Exception('*******WEIRED SIZE TYPE: {}*********'.format(size_type))

        total_size_in_gb += store_size_in_gb
        total_doc_count += docs_count
        print 'Store size in GB: {}, socs count: {}'.format(store_size_in_gb, docs_count)

        index_creation_timestamp = \
            requests.get('http://{}:{}/{}/_settings'.format(host, port, index_name)).json()[index_name]['settings'][
                'index']['creation_date']
        index_creation_datetime = datetime.fromtimestamp(float(index_creation_timestamp) / 1000.0)
        index_creation_string = '{}/{}/{}'.format(index_creation_datetime.day, index_creation_datetime.month,
                                                  index_creation_datetime.year)
        print 'Index creation date: {}'.format(index_creation_string)

        if (oldest_index_creation_data is None) or (index_creation_datetime < oldest_index_creation_data):
            oldest_index_creation_data = index_creation_datetime
            oldest_index_name = index_name

        if (youngest_index_creation_date is None) or (index_creation_datetime > youngest_index_creation_date):
            youngest_index_creation_date = index_creation_datetime
            youngest_index_name = index_name

if total_indices_count == 0:
    print 'No matching indices'
else:
    total_size_in_kb = float(total_size_in_gb) * 1024 * 1024
    avg_doc_size_in_kb = 0 if (total_doc_count == 0) else (
            total_size_in_kb / float(total_doc_count))
    avg_index_size_in_kb = total_size_in_kb / float(total_indices_count)
    avg_doc_count_in_index = float(total_doc_count) / float(total_indices_count)

    print
    print '******************************************************************'
    print
    print 'Total number of matching indices: {}'.format(total_indices_count)
    print 'Total size of all indices( include replicas): {} KB ({} GB)'.format(total_size_in_kb, total_size_in_gb)
    print 'Total number of docs( NOT includes replicas): {} KB'.format(total_doc_count)
    print
    print 'Average doc size( include replicas): {} KB'.format(avg_doc_size_in_kb)
    print 'Average index size( include replicas): {} KB ({} FB)'.format(avg_doc_size_in_kb, total_size_in_gb)
    print 'Average number of docs in index( NOT includes replicas): {} docs'.format(avg_doc_count_in_index)
    print
    print 'Oldest index in the template (created on:{}, {} days old): {}'.format(oldest_index_creation_data, (
            datetime.now() - oldest_index_creation_data).days, oldest_index_name)
    print 'Youngest index in the template (created on:{}, {} days old): {}'.format(youngest_index_creation_date, (
            datetime.now() - youngest_index_creation_date).days, youngest_index_name)
    print
    print '******************************************************************'
    print

    if matching_indices_closed:
        print 'Indices matching search but closed:\n\n{}'.format('\n'.join(matching_indices_closed))

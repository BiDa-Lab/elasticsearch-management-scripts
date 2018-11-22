import requests

print 'This script catch and print the size of the cluster state'
print

host = raw_input('Enter the Elasticsearch host: ')
port = raw_input('Enter the Elasticsearch port: ')
print

print 'Download the cluster state'
print

cluster_state = requests.get('http://{}:{}/_cluster/state'.format(host, port)).text
print 'The length in characters:{}'.format(len(cluster_state))

cluster_state_in_bytes = cluster_state.encode('utf-8')
if type(cluster_state_in_bytes) is str:
    print 'The cluster state size is : {}Mb'.format(float(len(cluster_state_in_bytes)) / 1024 / 1024)
else:
    print 'ERROR: Could\'t convert cluster state size to bytes!'

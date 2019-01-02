import csv
import requests
import json
from elasticsearch import Elasticsearch

ES_URI_CAT_NODES = 'http://{}:{}/_cat/nodes?v'
ES_URI_CAT_SHARDS = 'http://{}:{}/_cat/shards?v&h=index,state,shard'


class AllocateUnassignShards:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.es_uri_cat_nodes = ES_URI_CAT_NODES
        self.es_uri_cat_shards = ES_URI_CAT_SHARDS

    def es_elastic_version_updated(self):
        es_url = 'http://{}:{}'.format(self.host, self.port)
        response = {}
        try:
            response = requests.get(es_url)
        except requests.exceptions.RequestException as exception_msg:
            print exception_msg
            exit(1)
        response = response.json()
        version = response['version']['number']
        major = version.split('.', 1)[0]
        return int(major) >= 2

    def get_node_list(self):
        es_handler = Elasticsearch(self.host)
        nodes_list = es_handler.cat.nodes(format="json")
        data_nodes_list = [node['name'] for node in nodes_list if 'd' in node['node.role']]

        return data_nodes_list

    def shard_allocation(self):
        list_of_nodes = self.get_node_list()
        csv_lines = []
        count_of_nodes = 0

        try:
            csv_lines = list(
                csv.reader(requests.get(ES_URI_CAT_SHARDS.format(self.host, self.port, )).text.splitlines(),
                           delimimiter=' ', skipinitiaspace=True)
            )
        except requests.exceptions.RequestException as exception_msg:
            print exception_msg
        exit(1)

        headers = csv_lines[0]
        index_header_pos = headers.index('index')
        shard_header_pos = headers.index('shard')
        shard_state_header_pos = headers.index('state')

        indices_info = csv_lines[1:]

        for index_info in indices_info:
            shard_state = index_info[shard_state_header_pos]
            if shard_state.lower() == 'unassigned':
                index_name = index_info[index_header_pos]
                shard_number = index_info[shard_header_pos]
                node_to_reroute_to = list_of_nodes[count_of_nodes %
                                                   len(list_of_nodes)
                                                   ]
                count_of_nodes += 1
                allocate_empty_primary = {
                    "index": index_name,
                    "shard": shard_number,
                    "node": node_to_reroute_to,
                    "accept_data_loss": True
                }
                json_to_post = {
                    "commands": [
                        dict(allocate_empty_primary=allocate_empty_primary)
                    ]
                }

                print 'Rerouting shard {} of index {} to node {}.'.format(shard_number, index_name, node_to_reroute_to)
                response = requests.post('http://{}:{}/_cluster/reroute'.format(self.host, self.port),
                                         data=json.dumps(json_to_post),
                                         headers = {"content-type": "application/json"}
                                         )
                print 'Status code: {}'.format(response.status_code)
                if response.status_code != 200:
                    print 'Failed relocating shard, Response content: {}'.format(response.content)
                print
        print 'Done relocation'
    @staticmethod
    def welcome():
        print "******************************************************"
        print "This program monitor over the un-assigned shards"
        print "and move them to another connected node in the cluster"
        print "Possibly supports only ES ver 5.X"
        print "******************************************************"

    def main(self):
        AllocateUnassignShards.welcome()
        host = raw_input('Enter the Host')
        port = raw_input('Enter the Port')
        if port is None:
            port = 9200
        class_obj = AllocateUnassignShards
        class_obj.shard_allocation()

    if __name__ == '__main__':
        main()
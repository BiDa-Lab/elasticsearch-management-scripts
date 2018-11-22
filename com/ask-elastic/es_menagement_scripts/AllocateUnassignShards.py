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
        self.es_uri_cat_nodes= ES_URI_CAT_NODES
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

        return  data_nodes_list


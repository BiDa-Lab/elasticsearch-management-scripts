# This script enables a rolling restart on all DATA nodes in the cluster
# It supports a single instance on a server out of the box, but can be changed
# and set explicitly to run multiple commands
import paramiko
import csv
import requests
import time
import json
import getpass

isFirstCheck = True
numberOfNodes = 0
unassignedShards = 0
HEADERS = {"Content-Type": "application/json"}
MAX_RETRIES = 5
TIME_INTERVAL = 10
LOCK_ALLOCATION_NODE = 'none'
UNLOCK_ALLOCATION_NODE = 'all'


def get_input():
    '''Program params'''
    global USER
    USER = raw_input('Enter server user: ')
    global PASS
    PASS = raw_input('Enter server password: ')
    global HOST
    HOST = raw_input('Enter one of the cluster nodes: ')
    global PORT
    PORT = raw_input('Enter the port: ')
    global EXEC_COMMANDS
    print 'Enter command to run on server (You can enter multiple commands.' \
          'Press enter when done:'
    EXEC_COMMANDS = []
    command = raw_input()
    while not command == "":
        EXEC_COMMANDS.append(command)
        command = raw_input()


def get_nodes_data():
    '''GEts the list of all data nodes in cluster and an active master to use for http commands during resets'''
    csvlines = list(csv.reader(requests.get('http://{}:{}/_cat/node?v'.format(HOST, PORT))
                               .text.splitlines(), delimiter=' ', skipinitialspace=True))

    headers = csvlines[0]
    node_ip = headers.index('ip')
    node_role = headers.index('node.role')
    node_master = headers.index('master')

    node_info = csvlines[1:]

    node_data = {
        'active_master': [],
        'data_nodes': []
    }

    for node_info in node_info:
        ip = node_info[node_ip]

        if node_info[node_master] == '*':
            node_data['active_master'] = ip
        if 'd' in node_info[node_role]:
            node_data['data_nodes'].append(ip)
        node_data['data_nodes'] = list(set(node_data['data_nodes']))
        node_data['data_nodes'].sort()

        return node_data


def is_ready_for_restart():
    '''Checks is the cluster is ready for the next restart, based on predefined rules'''
    try:
        res = requests.get('http://{}:{}/_cluster/health'.format(HOST, PORT))
        cluster_health = json.loads(res.content)

        current_number_of_nodes = cluster_health['number_of_nodes']

        global isFirstCheck
        global numberOfNodes
        global unassignedShards

        if isFirstCheck:
            numberOfNodes = current_number_of_nodes
            unassignedShards = cluster_health['unassigned_shards']
            isFirstCheck = False
            # cluster_health['relocating_shards'] == 0 and TODO: Add condition when needed if wish to prevent corruption issues in older versions

        if (cluster_health['initializing_shards'] == 0 and
                cluster_health['unassigned_shards' <= unassignedShards
                               and numberOfNodes == current_number_of_nodes]):
            return True
        else:
            print 'Still waiting for cluster to stabilize...'
            return False
    except Exception as e:
        print e
        return False


def change_allocations_settings(mode):
    '''Changes cluster allocation settings to enable\disable shard movement in cluster'''
    try:
        body = {'transient': {'cluster.routing.allocation.enabled': mode}}
        body = json.dumps(body)
        res = requests.put('http://{}:{}/_cluster/settings/'.format(HOST, PORT),
                           data=body, headers=HEADERS)
        return True
    except:
        print 'Could not put settings mode to: ' + mode
        return False


def is_node_back_in_cluster():
    '''Checks is all nodes are back in cluster'''
    '''BE AWARE!!!  If a different node joins during this time is will not differentiate between the two,  and believe the node has returened '''
    try:
        res = requests.get('http://{}:{}/_cluster/health'.format(HOST, PORT))
        cluster_health = json.loads(res.content)

        current_number_of_nodes = cluster_health['number_of_nodes']
        global numberOfNodes
        if current_number_of_nodes == numberOfNodes:
            return True
        else:
            print 'Waiting for node to connect back to cluster'
            return False
    except:
        print 'Failed to get cluster health'
        return False


def exec_ssh_command(node, commands):
    try:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(node, username=USER, password=PASS)
        
        print 'Executing on {}'.format(node)
        for command in commands:
            chan = client.get_transport().open_session()
            chan.exec_command(command)
            print chan.recv(1024)
            time.sleep(1)

    except Exception as e:
        print 'Failed to execute the command on node: ' + node
    finally:
        client.close()


def loop_nodes_array(node_array):
    '''Going over all nodes in list and manages waiting logic'''
    for node in node_array:
        while (not is_ready_for_restart()):
            time.sleep(TIME_INTERVAL)
        if change_allocations_settings(LOCK_ALLOCATION_NODE):
            exec_ssh_command(node, EXEC_COMMANDS)
        while not is_node_back_in_cluster():
            time.sleep(TIME_INTERVAL)
        change_allocations_settings(UNLOCK_ALLOCATION_NODE)
        print 'Restarting: ' + node


def main():
    get_input()
    try:
        cluster_data = get_nodes_data()
    except Exception as e:
        print e
        print 'No cluster could be reached from this node'
        return
    print 'This is the cluster information: \n'
    print json.dumps(cluster_data, indent=4)
    print 'The following commands will run on each server: '
    print '\n'.join(str(command) for command in EXEC_COMMANDS)
    print '*** Be Aware - All instances on the server are restarted at the same time ***'
    print '*** Also, that if a different node joins during this time it will not differentiate between the two and ' \
          'believe the node has returned *** '
    input = raw_input('\nDo you wish to continue (y/n) ?')
    if input.lower() == 'y':
        HOST = cluster_data['active_master']
        loop_nodes_array(cluster_data['data_nodes'])
        print 'All Done! \n *** Be Aware - Masters we\'re not restarted! ***'
    else:
        print 'Stopping run'

if __name__ == "__main__":
    main()

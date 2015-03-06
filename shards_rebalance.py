#!/usr/bin/python

import sys
import logging
import re
import operator
from elasticsearch import Elasticsearch
from optparse import OptionParser

logging.basicConfig()

def get_last(es, name):
  i_list = es.cat.indices(index = name).split('\n')
  indices = []
  for s in i_list:
    try:
      indices.append(s.split(" ")[1])
    except:
      pass
  return sorted(indices, reverse=True)[0]

def get_nodes(es):
  nodes = []
  i_list = es.cat.nodes().split('\n')
  for s in i_list:
    fields = re.split('\s+', s)
    try:
      type = fields[5]
      node = fields[0]
      if type == 'd':
        nodes.append(node)
    except:
      pass
  return nodes

def get_shards(es, index, nodes):
  shards = []
  shards_num = { 'p': 0, 'r': 0}
  for name in nodes:
    node = {}
    node['name'] = name
    node['r'] = []
    node['p'] = []
    node['moving'] = []
    shards.append(node)

  i_list = es.cat.shards(index = index).split('\n')

  for s in i_list:
    try:
      fields = re.split('\s+', s)
      num = fields[1]
      cur_state = fields[2]
      name = fields[7]
    except:
      continue

    if cur_state == 'p' or cur_state == 'r':
      shards_num[cur_state] += 1
      node = filter(lambda x: x.get('name') == name, shards)[0]
      node[cur_state].append(num)

  return shards_num, shards

def rebalance(es, index, shards, state, factor):
  #print "factor", factor
  for node in shards:
    #print node['name'], len(node[state])
    flag = True
    while flag and len(node[state]) > factor:
      # look for node
      flag = False
      for free_node in sorted(shards,cmp=lambda x,y: cmp(len(x['p']),len(y['p'])) if cmp(len(x['p']),len(y['p'])) != 0 else cmp(len(x['r']),len(y['r']))):
        if len(free_node[state]) < factor:
          shard = node[state].pop()
          if shard_exist(shard, free_node):
            node[state].append(shard)
            continue

          relocate(es, index, shard, node['name'],free_node['name'])
          free_node[state].append(shard)
          node['moving'].append(shard)
          flag = True
          break

def shard_exist(shard, node):
  if shard in node['p'] or shard in node['r'] or shard in node['r'] or shard in node['moving']:
    return True
  return False

def relocate(es, index, shard, from_node, to_node):
  print "relocate " + index + ':' + shard + " from "  + from_node + " to " + to_node
  if options.dry:
    print " - dry run mode"
  else:
    es.cluster.reroute(body = '{ "commands" : [ { "move" : { "index" : "' + index + '", "shard" : ' + shard + ', "from_node" : "' + from_node + '", "to_node" : "' + to_node + '" } } ] }')

def all_started(es):
  i_list = es.cat.shards(h = [ 'state' ]).split('\n')
  for s in i_list:
    state = re.sub("\s+", "", s)
    if state != '' and state != 'STARTED':
      raise IOError('not all shards STARTED: found ' + state)
      return False
  return True

def main():
  global options
  parser = OptionParser()
  parser.add_option("-i", "--index", dest="index_list", default="",
                    help="list of indexes to rebalance")
  parser.add_option("-r", "--recent_index", dest="recent_index", default="",
                    help="process recent index from wildcards")
  parser.add_option("-d", dest="dry", action="store_true",
                    help="dry run")
  parser.add_option("-f", dest="force", action="store_true",
                    help="force to relocate even if other relocations active")
  parser.add_option("-s", dest="host", default="localhost",
                    help="ES server, default islocalhost")

  (options, args) = parser.parse_args()

  es = Elasticsearch([options.host],
			 #sniff_on_start=True,
                         max_retries=100,
			 timeout=60,
                         retry_on_timeout=True,
                         sniff_on_connection_fail=True,
                         sniff_timeout=100)

  status = es.cluster.health()["status"]
  if status != 'green':
    raise IOError('ES cluster status is ' + status)


  if not options.force and not all_started(es):
    raise IOError('not all shards STARTED')

  nodes = get_nodes(es)

  indexes = []

  if options.index_list != '':
    indexes = re.split('\s+', options.index_list)

  if options.recent_index != '':
    for i in re.split('\s+', options.recent_index):
      indexes.append(get_last(es, [ i ]))
    
  print "process indexes: ", indexes

  for index in indexes:
    (shards_num, shards) = get_shards(es, index, nodes)
    rebalance(es, index, shards, 'p', shards_num['p'] / len(nodes) + 1)
    rebalance(es, index, shards, 'r', shards_num['r'] / len(nodes) + 1)

  sys.exit(0)

if __name__ == '__main__':
    main()

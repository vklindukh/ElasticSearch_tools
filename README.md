# ElasticSearch_tools
Useful administrative tools for ES cluster

shards_rebalance - rebalance shards evenly across data nodes.

Usage: shards_rebalance.py [options]

Options:
  -h, --help            show this help message and exit
  -i INDEX_LIST, --index=INDEX_LIST
                        list of indexes to rebalance
  -r RECENT_INDEX, --recent_index=RECENT_INDEX
                        process recent index from wildcards
  -f                    dry run
  -s HOST               ES server, default islocalhost


Example:

./shards_rebalance.py -i 'my_index1 my_index5'
./shards_rebalance.py -r 'my_index* your_index*'


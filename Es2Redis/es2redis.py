from elasticsearch import Elasticsearch
from rediscluster import RedisCluster
from datetime import datetime, timedelta
import sys
import string, random


# need python3
# need pip install elasticsearch
# need pip install redis-py-cluster


def read_es(start, end, index):
    """
    read data from es
    :param start: the "time" start, default format is 'yyyy-MM-dd HH:mm:ss'
    :param end: the "time" end
    :param index: index name in es
    :return: key value tuple of list, [(k1, v1), (k2, v2)...]
    """
    doc = {
        "query": {
            "range": {
                "create_time": {
                    "gte": str(start),
                    "lte": str(end)
                }
            }
        }
    }
    es = Elasticsearch(['192.168.199.101'], http_auth=('admin', 'admin'), scheme="http", port=9200, )
    result = es.search(index=index, body=doc)

    list_keys = []
    for document in result["hits"]["hits"]:
        list_keys.append((document["_source"]["real_redis_key"], document["_source"]["real_redis_value"]))

    print("get {} docs from {}, time start {}, time end {}.".format(len(list_keys), index, start, end))
    # store to redis
    return list_keys


def store_to_redis(kvlist):
    """
    Store kv list data to redis
    """
    # Init redis client
    startup_nodes = [{"host": "192.168.199.100", "port": "7001"}, {"host": "192.168.199.100", "port": "7002"},
                     {"host": "192.168.199.100", "port": "7003"}, {"host": "192.168.199.101", "port": "7001"},
                     {"host": "192.168.199.101", "port": "7002"}, {"host": "192.168.199.101", "port": "7003"}]
    rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

    count = 0
    for k, v in kvlist:
        rc.set(k, v)
        count += 1

    print("success store {}.".format(count))


def random_id(size=16, chars=string.ascii_uppercase + string.digits):
    """
    Generates a random id based on `size` and `chars` variable.

    By default it will generate a 16 character long string based on
    ascii uppercase letters and digits.
    """
    return ''.join(random.choice(chars) for _ in range(size))


if __name__ == '__main__':
    # get arguments
    time_start = sys.argv[1]
    hour_duration = sys.argv[2]
    index = sys.argv[3]

    # calculate start/end time
    start_time = datetime.strptime(time_start, "%Y-%m-%d %H:%M:%S")
    start_time = datetime(start_time.year, start_time.month, start_time.day, start_time.hour, 0, 0)
    end_time = start_time + timedelta(hours=int(hour_duration))

    # read es
    es_docs = read_es(start_time, end_time, index)

    # store to redis
    store_to_redis(es_docs)

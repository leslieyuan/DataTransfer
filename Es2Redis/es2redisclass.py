from elasticsearch import Elasticsearch
from rediscluster import RedisCluster


class ReadEs2Redis:
    """
    Read data from elasticsearch, store to redis
    """

    def __init__(self, stime, etime, es_index):
        self._start_time = stime
        self._end_time = etime
        self._es_index = es_index

    def read_es(self):
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
                        "gte": str(self._start_time),
                        "lte": str(self._end_time)
                    }
                }
            }
        }
        es = Elasticsearch(['192.168.199.101'], http_auth=('admin', 'admin'), scheme="http", port=9200, )
        result = es.search(index=self._es_index, body=doc)

        list_keys = []
        for document in result["hits"]["hits"]:
            list_keys.append((document["_source"]["real_redis_key"], document["_source"]["real_redis_value"]))

        print(
            "get {} docs from {}, time start {}, time end {}.".format(len(list_keys), self._start_time, self._end_time,
                                                                      self._es_index))
        # store to redis
        return list_keys

    @classmethod
    def store_to_redis(cls, kvlist):
        """
        Store kv list data to redis
        """
        # Init redis client
        startup_nodes = [{"host": "192.168.199.100", "port": "7001"}, {"host": "192.168.199.100", "port": "7002"},
                         {"host": "192.168.199.100", "port": "7003"}, {"host": "192.168.199.101", "port": "7001"},
                         {"host": "192.168.199.101", "port": "7002"}, {"host": "192.168.199.101", "port": "7003"}]
        rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

        for k, v in kvlist:
            rc.set(k, v)

        print("success store {}.".format(len(kvlist)))


if __name__ == '__main__':
    import sys
    from datetime import datetime, timedelta

    # get arguments
    time_start = sys.argv[1]
    hour_duration = sys.argv[2]
    index = sys.argv[3]

    # calculate start/end time
    start_time = datetime.strptime(time_start, "%Y-%m-%d %H:%M:%S")
    start_time = datetime(start_time.year, start_time.month, start_time.day, start_time.hour, 0, 0)
    end_time = start_time + timedelta(hours=int(hour_duration))

    # read es
    read2Redis = ReadEs2Redis(start_time, end_time, index)
    es_list = read2Redis.read_es()
    ReadEs2Redis.store_to_redis(es_list)

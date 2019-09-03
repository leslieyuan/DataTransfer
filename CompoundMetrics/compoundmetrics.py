""" A script for generate compound metrics.

This script run each 1 minute.
It will build a redis client and read the rules, then generate compound metrics to redis.
"""

from datetime import datetime


REDIS_NODES = [{"host": "192.168.199.100", "port": "7001"}, {"host": "192.168.199.100", "port": "7002"},
                     {"host": "192.168.199.100", "port": "7003"}, {"host": "192.168.199.101", "port": "7001"},
                     {"host": "192.168.199.101", "port": "7002"}, {"host": "192.168.199.101", "port": "7003"}]


class CompoundMetrics:
    def __init__(self, rule_head=None):
        try:
            from rediscluster import RedisCluster
        except ImportError:
            raise Exception('Import exception, please install rediscluster first.')
        # init redis cluster client
        self.rc = RedisCluster(startup_nodes=REDIS_NODES, decode_responses=True)
        curtime = datetime.now()
        self.curtime_nosecond = datetime(curtime.year, curtime.month, curtime.day, curtime.hour, curtime.minute, 0)
        self.rule_pattern = rule_head + '*'
        self.rule_lists = []

    def read_rules(self):
        # Get all rule redis keys
        if not self.rule_pattern:
            raise Exception('Need set up rule head string.')
        all_keys = self.rc.keys(self.rule_pattern)

        # Read rules from redis

        for key in all_keys:
            one_rule = self.rc.get(key)
            self.rule_lists.append(one_rule)

    def gen_metrics_by_rules(self):
        # Todo:

        pass


if __name__ == '__main__':
    cm = CompoundMetrics("snc_compound_metric#")
    cm.read_rules()
    cm.gen_metrics_by_rules()


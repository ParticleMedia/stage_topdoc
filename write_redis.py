import logging
import argparse
import redis
import json

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : [WriteRedis] %(message)s',
    level=logging.INFO)


def main():
	arg_parser = argparse.ArgumentParser()
	arg_parser.add_argument('--input', help='To save key value file, each line format: key value ttl(second)')
	arg_parser.add_argument('--prefix', help='prefix')
	arg_parser.add_argument('--stage', type=int, default=100000, help='check stage')
	arg_parser.add_argument('--days', type=int, default=3, help='statistic time range')
	arg_parser.add_argument('--ttl', type=int, default=172800, help='expire ttl, default 2days')
	args = arg_parser.parse_args()
	
	prefix = args.prefix
	stage = args.stage
	days = args.days
	logging.info('Start.')

	rc = redis.Redis(host="proxy.cfb.redisc.nb.com", port=6379)
	pipe = rc.pipeline()

	idx = 0
	key = '%s#%s#%sd' % (prefix, stage, days)
	result = {}
	with open(args.input, 'r') as f:
		for idx, line in enumerate(f, start=1):
			docid, ctr = line.strip().split(',')
			result[docid] = ctr
		rc.setex(key, args.ttl, json.dumps(result))
		pipe.execute()
		logging.info(str(idx))

	logging.info('Finish.')


if __name__ == '__main__':
	main()
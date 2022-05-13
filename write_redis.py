import logging
import argparse
import redis


logging.basicConfig(
    format='%(asctime)s : %(levelname)s : [WriteRedis] %(message)s',
    level=logging.INFO)


def main():
	arg_parser = argparse.ArgumentParser()
	arg_parser.add_argument('--input', help='To save key value file, each line format: key value ttl(second)')
	arg_parser.add_argument('--prefix', help='key prefix')
	arg_parser.add_argument('--ttl', type=int, default=172800, help='expire ttl, default 2days')
	args = arg_parser.parse_args()
	
	prefix = args.prefix
	logging.info('Start.')

	rc = redis.Redis(host="proxy.cfb.redisc.nb.com", port=6379)
	pipe = rc.pipeline()

	idx = 0
	with open(args.input, 'r') as f:
		for idx, line in enumerate(f, start=1):
			key, value = line.strip().split()
			key = '%s@%s' % (prefix, key)

			rc.setex(key, args.ttl, value)

			if idx % 1000 == 0:
				pipe.execute()
			
			if idx % 10000 == 0:
				logging.info(str(idx))

		if idx % 1000 != 0:
			pipe.execute()
		logging.info(str(idx))

	logging.info('Finish.')


if __name__ == '__main__':
	main()
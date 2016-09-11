#!/usr/bin/env python
from pg_chameleon import replica_engine
commands=['init_schema','init_replica','start_replica']
import argparse
parser = argparse.ArgumentParser(description='Command line for pg_chameleon.')
parser.add_argument('command', metavar='N', type=str,  help='Command accepted, '+','.join(commands))
args = parser.parse_args()

if args.command in commands:
	replica=replica_engine()
	if args.command == commands[0]:
		replica.create_service_schema(cleanup=True)
	elif args.command == commands[1]:
		replica.create_tables(drop_tables=True)
		replica.copy_table_data(table_limit=100000)
		replica.create_indices()
	elif args.command == commands[2]:
		replica.do_stream_data()
	else:
		print args.command.help

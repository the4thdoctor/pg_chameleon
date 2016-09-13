#!/usr/bin/env python
from pg_chameleon import replica_engine
commands=['init_schema','init_replica','start_replica']
command_help='Available commands, '+','.join(commands)
import argparse
parser = argparse.ArgumentParser(description='Command line for pg_chameleon.')
parser.add_argument('command', metavar='command', type=str,  help=command_help)
args = parser.parse_args()

if args.command in commands:
	replica=replica_engine(args.command)
	if args.command == commands[0]:
		replica.create_service_schema(cleanup=True)
	elif args.command == commands[1]:
		replica.create_tables(drop_tables=True)
		replica.copy_table_data()
		replica.create_indices()
	elif args.command == commands[2]:
		replica.do_stream_data()
	else:
		print command_help

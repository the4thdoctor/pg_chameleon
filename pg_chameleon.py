#!/usr/bin/env python
import argparse
from pg_chameleon import replica_engine
commands = [
					'create_schema',
					'init_replica',
					'start_replica',
					'upgrade_schema',
					'drop_schema'
	]
command_help = 'Available commands, ' + ','.join(commands)

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', metavar='command', type=str, help=command_help)
args = parser.parse_args()


if args.command in commands:
	replica = replica_engine(args.command)
	if args.command == commands[0]:
		replica.create_service_schema()
	elif args.command == commands[1]:
		replica.drop_service_schema()
		replica.create_service_schema()
		replica.create_schema()
		replica.copy_table_data()
		replica.create_indices()
	elif args.command == commands[2]:
		replica.run_replica()
	elif args.command == commands[3]:
		replica.upgrade_service_schema()
	elif args.command == commands[4]:
		replica.drop_service_schema()

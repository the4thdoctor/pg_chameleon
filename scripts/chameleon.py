#!/usr/bin/env python
from pkg_resources import get_distribution
__version__ = get_distribution('pg_chameleon').version
import argparse
from pg_chameleon import replica_engine
from pg_chameleon import config_dir
configdir = config_dir()
configdir.set_config()

commands = [
	'create_schema',
	'init_replica',
	'start_replica',
	'upgrade_schema',
	'drop_schema', 
	'list_config', 
	'add_source', 
	'drop_source', 
	'stop_replica', 
	'disable_replica', 
	'enable_replica', 
	'sync_tables', 
	'show_status' , 
	'detach_replica', 
	'set_config'
	]
command_help = 'Available commands, ' + ','.join(commands)
table_help =  """Specifies the table's name to sync. It's possible to specify multiple table names separated by comma. If the parameter is omitted all tables will be syncronised."""
config_help =  """Specifies the configuration to use. The configuration shall be specified without extension (e.g. --config foo) and the file foo.yaml should be in ~/.pg_chameleon/config/"""
debug_help = """Enables the debug mode.The log output is stdout and in debug verbosity. The lock file creation on error is skipped as well."""
nolock_help = """Do not create the lock file on error, allowing the replica to be restarted immediately. """
thread_help = """  When specified the replica process runs the read and replay in two threads."""

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', metavar='command', type=str, help=command_help)
parser.add_argument('--config', metavar='config', type=str,  default='default',  required=False, help=config_help)
parser.add_argument('--table', metavar='table', type=str,  default='*',  required=False, help=table_help)
parser.add_argument('--debug',  default=False,  required=False, help=nolock_help,  action='store_true')
parser.add_argument('--nolock',  default=False,  required=False, help=debug_help,  action='store_true')
parser.add_argument('--version', action='version',version='pg_chameleon {version}'.format(version=__version__))
parser.add_argument('--thread',  default=False,  required=False, help=thread_help,  action='store_true')
args = parser.parse_args()

if args.command == commands[14]:
		configdir.set_config()
elif args.command in commands:
	
	replica = replica_engine(args.config, args.debug, args.nolock)
	if args.command == commands[0]:
		replica.create_service_schema()
	elif args.command == commands[1]:
		replica.init_replica()
	elif args.command == commands[2]:
		if args.thread:
			replica.run_replica_thread()
		else:
			replica.run_replica()
	elif args.command == commands[3]:
		replica.upgrade_service_schema()
	elif args.command == commands[4]:
		replica.drop_service_schema()
	elif args.command == commands[5]:
		replica.list_config()
	elif args.command == commands[6]:
		replica.add_source()
	elif args.command == commands[7]:
		replica.drop_source()
	elif args.command == commands[8]:
		replica.stop_replica()
	elif args.command == commands[9]:
		replica.stop_replica(allow_restart=False)
	elif args.command == commands[10]:
		replica.enable_replica()
	elif args.command == commands[11]:
		replica.sync_tables(args.table)
	elif args.command == commands[12]:
		replica.show_status()
	elif args.command == commands[13]:
		replica.detach_replica()
	

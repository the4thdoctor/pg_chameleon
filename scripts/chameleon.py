#!/usr/bin/env python
from pkg_resources import get_distribution
__version__ = get_distribution('pg_chameleon')
import argparse
from pg_chameleon import replica_engine

commands = [
	'show_config',
	'show_sources',
	'show_status', 
	'create_replica_schema',
	'drop_replica_schema',
	'upgrade_replica_schema',
	'add_source',
	'drop_source',
	'init_replica',
	'update_schema_mappings',
	'refresh_schema',
	'sync_tables',
	'start_replica', 
	'stop_replica', 
	'detach_replica', 
	'set_configuration_files'
	]

command_help = """Available commands, """ + ','.join(commands)
config_help = """Specifies the configuration to use without the suffix yml. If  the parameter is omitted then ~/.pg_chameleon/configuration/default.yml is used"""
schema_help = """Specifies the schema within a source. If omitted all schemas for the given source are affected by the command. Requires the argument --source to be specified"""
source_help = """Specifies the source within a configuration. If omitted all sources are affected by the command."""
tables_help = """Specifies the tables within a source . If omitted all tables are affected by the command."""
debug_help = """Forces the debug mode with logging on stdout and log level debug."""
version_help = """Displays pg_chameleon's installed  version."""

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--config', type=str,  default='default',  required=False, help=config_help)
parser.add_argument('--schema', type=str,  default='*',  required=False, help=schema_help)
parser.add_argument('--source', type=str,  default='*',  required=False, help=source_help)
parser.add_argument('--tables', type=str,  default='*',  required=False, help=tables_help)
parser.add_argument('--debug', default=False, required=False, help=debug_help, action='store_true')
parser.add_argument('--version', action='version', help=version_help,version='{version}'.format(version=__version__))

args = parser.parse_args()


replica = replica_engine(args)
getattr(replica, args.command)()

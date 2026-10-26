#!/usr/bin/env python
from importlib.metadata import version
__version__ = version('pg_chameleon')
import argparse
from pg_chameleon import replica_engine

commands = [
    'show_config',
    ]

command_help = ','.join(commands)
config_file_help = """Specifies the configuration file full path. Supports the shell expansion like ~ """
# schema_help = """Specifies the schema within a source. If omitted all schemas for the given source are affected by the command. Requires the argument --source to be specified"""
# source_help = """Specifies the source within a configuration. If omitted all sources are affected by the command."""
# tables_help = """Specifies the tables within a source . If omitted all tables are affected by the command."""
# logid_help = """Specifies the log id entry for displaying the error details"""
# debug_help = """Forces the debug mode with logging on stdout and log level debug."""
# version_help = """Displays pg_chameleon's installed  version."""
# full_help = """When specified with run_maintenance the switch performs a vacuum full instead of a normal vacuum. """
# truncate_help = """Truncate the existing tables instead of replacing them."""

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--config_file', type=str,  default=None,  required=False, help=config_file_help)
# parser.add_argument('--schema', type=str,  default='*',  required=False, help=schema_help)
# parser.add_argument('--source', type=str,  default='*',  required=False, help=source_help)
# parser.add_argument('--tables', type=str,  default='*',  required=False, help=tables_help)
# parser.add_argument('--logid', type=str,  default='*',  required=False, help=logid_help)
# parser.add_argument('--debug', default=False, required=False, help=debug_help, action='store_true')
# parser.add_argument('--full', default=False, required=False, help=full_help, action='store_true')
# parser.add_argument('--version', action='version', help=version_help,version='{version}'.format(version=__version__))
args = parser.parse_args()

print(args)
#
replica = replica_engine(args)

# if args.debug:
#     getattr(replica, args.command)()
# else:
#     try:
#         getattr(replica, args.command)()
#     except AttributeError:
#         print("ERROR - Invalid command" )
#         print(command_help)

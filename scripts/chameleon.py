#!/usr/bin/env python
from pkg_resources import get_distribution
__version__ = get_distribution('pg_chameleon').version
import argparse
from pg_chameleon import replica_engine

commands = [
	'show_config',
	]

command_help = """Available commands, """ + ','.join(commands)
config_help = """Specifies the configuration to use. If  the parameter is omitted then it defaults to ~/.pg_chameleon/configuration/default.yml"""

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--config', type=str,  default='default',  required=False, help=config_help)

args = parser.parse_args()


replica = replica_engine(args)
getattr(replica, args.command)()

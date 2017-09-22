#!/usr/bin/env python
from pkg_resources import get_distribution
__version__ = get_distribution('pg_chameleon').version
import argparse
from pg_chameleon import replica_engine

commands = [
	'show_config',
	]

command_help = """Available commands, """ + ','.join(commands)

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
args = parser.parse_args()


replica = replica_engine(args)
getattr(replica, args.command)()

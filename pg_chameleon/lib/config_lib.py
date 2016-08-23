import yaml
import sys
import os
class config_lib:
	if not os.path.isfile(config_file):
		print "**FATAL - configuration file missing **\ncopy config/config-example.yaml to config/config.yaml and set your connection settings."
		sys.exit()

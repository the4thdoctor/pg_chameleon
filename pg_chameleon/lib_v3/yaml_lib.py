import yaml
from importlib.metadata import version as pkg_version

class yaml_lib:
    def __init__(self,config_file):
        self.config_file = config_file
        __config_file = open(self.config_file, 'r')
        pyyml = pkg_version('PyYAML')
        if pyyml <= '3.13':
            self.config = yaml.load(__config_file.read())
        else:
            self.config = yaml.load(__config_file.read(), Loader=yaml.FullLoader)
        __config_file.close()
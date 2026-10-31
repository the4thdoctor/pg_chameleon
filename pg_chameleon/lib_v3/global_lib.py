import os, os.path
import sys
from shutil import copy
from pg_chameleon import yaml_lib
from pg_chameleon import pgsql_lib
class replica_engine(object):
    """
        This class is wraps the the mysql and postgresql engines in order to perform the various activities required for the replica.
        The constructor inits the global configuration class  and setup the mysql and postgresql engines as class objects.
        The class sets the logging using the configuration parameter.

    """
    def __init__(self, args):
        """
            Class constructor.
        """
        if os.geteuid()==0:
            print ("pg_chameleon cannot be run as root")
            sys.exit(1)


        self.catalog_version = '3.0'
        self.upgradable_version = '2.0.10'
        self.lst_yes= ['yes',  'Yes', 'y', 'Y']
        python_lib=os.path.dirname(os.path.realpath(__file__))
        cham_dir = "%s/.config/pg_chameleon" % os.path.expanduser('~')
        local_conf = "%s/configuration/" % cham_dir
        local_logs = "%s/logs/" % cham_dir
        local_pid = "%s/pid/" % cham_dir

        self.__conf_dirs=[
            cham_dir,
            local_conf,
            local_logs,
            local_pid,

        ]

        self.__global_conf_example = '%s/../configuration/config-example.yml' % python_lib
        self.__config_example = '%s/config-example.yml' % local_conf



        self.__set_configuration_files()

        if args.config_file and os.path.isfile(args.config_file):
            self.config_file = args.config_file
        else:
            self.config_file = '%s/default.yml' % local_conf
        self.__yaml_lib = yaml_lib(self.config_file)
        # print(self.__yaml_lib.config)




    def __set_configuration_files(self):
        """
            The method loops the list self.conf_dirs creating them only if they are missing.

            The method checks the freshness of the config-example.yaml and connection-example.yml
            copies the new version from the python library determined in the class constructor with get_python_lib().

            If the configuration file is missing the method copies the file with a different message.

        """

        for confdir in self.__conf_dirs:
            if not os.path.isdir(confdir):
                print ("creating directory %s" % confdir)
                os.mkdir(confdir)


        if os.path.isfile(self.__config_example):
            if os.path.getctime(self.__global_conf_example)>os.path.getctime(self.__config_example):
                print ("updating configuration example with %s" % self.__config_example)
                copy(self.__global_conf_example, self.__config_example)
        else:
            print ("copying configuration  example in %s" % self.__config_example)
            copy(self.__global_conf_example, self.__config_example)

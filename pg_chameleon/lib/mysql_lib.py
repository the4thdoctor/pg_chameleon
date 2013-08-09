import os
import sys
from sqlalchemy  import create_engine,MetaData

class my_db_connection:
    """class to manage the mysql connection"""
    def __init__(self,t_conf_file=''):
        """Init function, initialises the class wide variables  """
        self.t_conn_str=''
        self.t_conf_file=t_conf_file
        self.dic_conn={}
        self.ob_engine=None
        self.ob_conn = None
        self.ob_metadata = None
        self.load_conf()
        self.build_conn_string()
        self.start_engine()
        
            
    def load_conf(self,):
        t_conf_file=self.t_conf_file
        if t_conf_file=='':
            t_cwd=os.getcwd()
            t_conf_file=t_cwd+'/config/my_connection.conf'
        try:
            f_conf=open(t_conf_file,'rb')
        except: 
            print "Couldn't load the configuration file in "+t_conf_file
            sys.exit(1) 
        t_line='x'
        while t_line:
            t_line=(f_conf.readline()).strip()
            try:
                if str(t_line)[0]!='#':
                    l_line=[]
                    if t_line!='':
                        l_line=t_line.split('=')
                        self.dic_conn[l_line[0]]=l_line[1]
            except:
                continue
            
    def build_conn_string(self):
        """builds the mysql connection string"""
        dic_conn=self.dic_conn
        self.t_conn_str = "mysql://"+dic_conn["dbuser"]+":"+dic_conn["dbpass"]+"@"+dic_conn["dbhost"]+"/"+dic_conn["dbname"]
        
        
    def start_engine(self):
        """start a new engine using the conn string"""
        try:
            self.ob_engine
            self.ob_engine = create_engine(self.t_conn_str, echo=False)
            self.ob_conn = self.ob_engine.connect()
            self.ob_metadata = MetaData(self.ob_engine,reflect=True)
        except:
            print self.t_conn_str
            
class my_data_def:
    """class for generating the ddl in the postgresql dialect """
    def __init__(self,l_args):
        """ init function accept the objects initiated in the my_db_connection class  """
        self.ob_engine=l_args[0]
        self.ob_conn = l_args[1]
        self.ob_metadata = l_args[2]
        self.l_tables=[]
        self.dic_datatype={'integer':'integer','mediumint':'int8','tinyint':'int2','smallint':'int2','int':'int8','varchar':'varchar','bigint':'int8','text':'text','char':'char','datetime':'date','longtext':'text','tinytext':'text','tinyblob':'bytea','mediumblob':'bytea','longblob':'bytea','blob':'bytea'}

    
    def clean_type_names(self,t_type):
        l_clean=[]
        t_clean=str(t_type).lower()
        l_clean=t_clean.split('(')
        t_clean=l_clean[0]
        return t_clean
    
    def build_column_list(self,ob_table):
        """ the function build the column definition for postgres """
        l_column=[]
        for column in ob_table.columns:
            l_args=[]
            l_args.append(str(column.name))
            l_args.append(str(column.type))
            l_args.append(column.nullable)
            t_col_def=''
            t_name=l_args[0]
            t_type=self.clean_type_names(l_args[1])
            b_null=l_args[2]
            if b_null:
                t_null='NULL'
            else:
                t_null='NOT NULL'
            try:
                t_type=self.dic_datatype[t_type]
            except:
                t_type='text'
            t_col_def=t_name+' '+t_type+' '+t_null
            l_column.append(t_col_def)
        return l_column
   
    def build_tab_list(self):
        """ function to get the tables from the mysql db """
        l_table=[]
        for ob_table in self.ob_metadata.sorted_tables:
            l_table=[ob_table.name,self.build_column_list(ob_table)]
            self.l_tables.append(l_table) 
        print self.l_tables
        
        
        
        
        
        
        
        
import csv
import os
import sys
from sqlalchemy  import create_engine,MetaData
from sqlalchemy.engine import reflection
import sqlalchemy
import codecs
import time
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
        self.t_conn_str = "mysql://"+dic_conn["dbuser"]+":"+dic_conn["dbpass"]+"@"+dic_conn["dbhost"]+"/"+dic_conn["dbname"]+"?charset=utf8"
        
        
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
    """class for aggregating data definition into a list to feed into the postgresql class """
    def __init__(self,l_args):
        """ init function accept the objects initiated in the my_db_connection class  """
        self.ob_engine=l_args[0]
        self.ob_conn = l_args[1]
        self.ob_metadata = l_args[2]
        self.l_tables=[]
        self.l_pkeys=[]
        self.dic_datatype={'integer':'integer','mediumint':'bigint','tinyint':'integer','smallint':'integer','int':'bigint','varchar':'varchar','bigint':'bigint','text':'text','char':'char','datetime':'date','longtext':'text','tinytext':'text','tinyblob':'bytea','mediumblob':'bytea','longblob':'bytea','blob':'bytea'}

    
    def clean_type_names(self,t_type):
        l_clean=[]
        t_clean=str(t_type).lower()
        l_clean=t_clean.split('(')
        t_clean=l_clean[0]
        try:
                t_clean=self.dic_datatype[t_clean]
        except:
                t_clean='text'
            
        if t_clean == 'char' or t_clean =='varchar':
            t_clean=t_clean+'('+l_clean[1]
        
        return t_clean
    
    def build_column_list(self,ob_table):
        """ the function builds the column definition for postgres """
        l_column=[]
        for column in ob_table.columns:
            l_args=[]
            l_args.append(str(column.name))
            l_args.append(str(column.type))
            l_args.append(column.nullable)
            l_col_def=[]
            t_name=l_args[0]
            t_type=self.clean_type_names(l_args[1])
            b_null=l_args[2]
            if b_null:
                t_null='NULL'
            else:
                t_null='NOT NULL'
                
            if (t_type =='integer' or t_type =='smallint' or t_type =='bigint') and column.autoincrement:
                t_auto='AUTOINC'
            else:    
                t_auto='NOAUTOINC'
            l_col_def=[t_name,t_type,t_null,t_auto]
            l_column.append(l_col_def)
        return l_column
   
    def build_tab_list(self):
        """ function to get the tables from the mysql db """
        ob_inspector = reflection.Inspector.from_engine(self.ob_engine)
        l_table=[]
        for ob_table in self.ob_metadata.sorted_tables:
            l_pkey=[]
            d_pkget=ob_inspector.get_pk_constraint(ob_table.name)
            l_pkey.append(ob_table.name)
            l_pkey.append(d_pkget)
            l_table=[ob_table.name,self.build_column_list(ob_table)]
            self.l_tables.append(l_table)
            self.l_pkeys.append(l_pkey) 
        
        
class my_data_flow:
    """class for managing the data flow from mysql """
    def __init__(self,l_args):
        self.ob_engine=l_args[0]
        self.ob_conn = l_args[1]
        self.ob_metadata = l_args[2]
        self.l_tables = l_args[3]
        self.t_out_dir= l_args[4]
        self.l_tab_file=[]
        
        
    def pull_data(self,i_limit=1000000):
        """ function to pull the data in copy format"""
        i_sequence=0
        for l_table in self.l_tables:
            print "pulling data from table "+l_table[0]+" with chunk size "+str(i_limit)
            t_sql_count="SELECT count(*) as i_cnt FROM "+l_table[0]+";"
            ob_res_count = self.ob_conn.execute(t_sql_count).fetchall()
            i_cnt=ob_res_count[0][0]
            i_num_read=1
            rng_num_read=range(1)
            if i_cnt>0:
                print "got "+str(i_cnt)+" records"
                i_num_read=i_cnt/i_limit
                rng_num_read=range(i_num_read+1)
                
            
            
            l_fields=[]
            for l_field in  l_table[1]:
                t_field="COALESCE(REPLACE("+l_field[0]+", '\"', '\"\"'),'NULL') "
                l_fields.append(t_field)
            
            v_fields="REPLACE(CONCAT('\"',CONCAT_WS('\",\"',"+','.join(l_fields)+"),'\"'),'\"NULL\"','NULL')"
            t_out_file=self.t_out_dir+'/out_data'+str(i_sequence)+'.csv'
            o_out_file= codecs.open(t_out_file,'wb',encoding='utf8')
            for rng_item in rng_num_read:
                t_sql="SELECT "+v_fields+" FROM "+l_table[0]+" LIMIT "+str(rng_item*i_limit)+", "+str(i_limit)+";"
                #print t_sql
                ob_result = self.ob_conn.execute(t_sql).fetchall()
                
                for l_row in ob_result:
                    try:
                        o_out_file.write(l_row[0]+"\n")
                    except:
                        print l_row[0]
                        raise Exception("error")
                print str(time.ctime())+" - "+str(min(i_cnt,(rng_item+1)*i_limit))+" records pulled"
            
            
            o_out_file.close() 
                
            
            l_out=[l_table[0],t_out_file]
            self.l_tab_file.append(l_out)
            i_sequence=i_sequence+1
        
        
        
        
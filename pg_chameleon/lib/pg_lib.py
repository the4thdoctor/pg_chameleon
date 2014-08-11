import os
import sys
from sqlalchemy import create_engine
import psycopg2
class pg_db_connection:
    """class to manage the postgresql connection"""
    def __init__(self,t_conf_file=''):
        """Init function, initialises the class wide variables  """
        self.t_conn_str=''
        self.t_conf_file=t_conf_file
        self.dic_conn={}
        self.ob_engine=None
        self.ob_conn = None
        self.load_conf()
        self.build_conn_string()
        self.start_engine()
        
            
    def load_conf(self,):
        t_conf_file=self.t_conf_file
        if t_conf_file=='':
            t_cwd=os.getcwd()
            t_conf_file=t_cwd+'/config/pg_connection.conf'
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
        """builds the PostgreSQL connection string"""
        dic_conn=self.dic_conn
        self.t_conn_str="postgresql+psycopg2://"+dic_conn["dbuser"]+":"+dic_conn["dbpass"]+"@"+dic_conn["dbhost"]+":"+dic_conn["dbport"]+"/"+dic_conn["dbname"]
    def start_engine(self):
        """start a new engine using the conn string"""
        try:
            self.ob_engine
            self.ob_engine = create_engine(self.t_conn_str, echo=False)
            self.ob_conn = self.ob_engine.connect()
        except:
            print self.t_conn_str    

class pg_data_def:
    """class for building the postgresql ddl statements """
    def __init__(self,l_args):
        """ init function accept the objects initiated in the pg_db_connection class  """
        self.l_tables=l_args[0]
        self.l_pkeys=l_args[1]
        self.l_indices=l_args[2]
        self.l_tables_def=[]
        self.l_pkeys_def=[]
        self.l_idx_def=[]
        self.build_indices_ddl()
        self.build_tab_ddl()
        self.build_pkeys_ddl()
        self.pg_conn=None
        
        
    def create_objects(self,l_args):
        """the function connects to postgres and run the ddls"""
        try:
            t_file_conf=l_args[0]
        except:
            t_file_conf=''
        try:
            b_drop=l_args[1]
        except:
            b_drop=False
        try:
            t_kind=l_args[2]
        except:
            t_kind='all'
        self.pg_conn=pg_db_connection(t_file_conf)
        if t_kind=='all' or t_kind=='tables':
            self.create_tables(b_drop)  
        if t_kind=='all' or t_kind=='pkeys':
            self.create_pkeys()
        if t_kind=='all' or t_kind=='idx':
            self.create_idx()
        
    
    def create_idx(self):
        print "start index build"    
        for t_idx in self.l_idx_def:
            print "creating index "+ t_idx[1]+ " on table "+ t_idx[0]
            self.pg_conn.ob_engine.execute(t_idx[2])
        print "done"
        
    def create_pkeys(self):
        print "start primary key build"    
        for t_pkey in self.l_pkeys_def:
            print "creating primary key "+ t_pkey[1]+ " on table "+ t_pkey[0]
            self.pg_conn.ob_engine.execute(t_pkey[2])
        print "done"
        
    def create_tables(self,b_drop):
        """ create the tables using the list l_tables_def  """
        for l_table_def in self.l_tables_def:
            t_table_def=l_table_def[1]
            if b_drop:
                print "dropping table "+l_table_def[0]
                t_table_drop='DROP TABLE IF EXISTS "'+l_table_def[0]+'" ;'
                self.pg_conn.ob_engine.execute(t_table_drop)
            t_table_def=l_table_def[1]
            print "creating table "+l_table_def[0]
            self.pg_conn.ob_engine.execute(t_table_def)
            
            
    def save_ddl(self,l_args):
        """the function writes the table and primary keys ddl in a file"""
        try:
            t_file_name=l_args[0]
        except:
            t_file_name='/tmp/db_schema.sql'
        try:
            b_drop=l_args[1]
        except:
            b_drop=False
        
        f_sql=open(t_file_name,'wb')
        for l_table_def in self.l_tables_def:
            t_table_def=l_table_def[1]
            if b_drop:
                t_table_drop='DROP TABLE IF EXISTS "'+l_table_def[0]+'" ;'
                f_sql.write(t_table_drop+"\n\n")
            f_sql.write(t_table_def+"\n\n")
            
        for t_pkey in self.l_pkeys_def:
            f_sql.write(t_pkey[2]+"\n\n")
            
        for t_idx in self.l_idx_def:
            f_sql.write(t_idx[2]+"\n\n")
        f_sql.close()
    
    
    def build_indices_ddl(self):
        """ builds the indices ddl """
        for l_index in self.l_indices:
            l_idx=[]
            t_table=l_index[0]
            d_idx_meta=l_index[1]
            for d_index in d_idx_meta:
                b_unique=d_index['unique']
                l_fields=d_index['column_names']
                t_name=d_index['name']
                t_idx_name='idx_'+t_table+'_'+str('_').join(l_fields)+'_'+t_name
                if b_unique:
                    t_unique=' UNIQUE '
                else:
                    t_unique=''
                
                t_idx_def='CREATE '+t_unique+' INDEX '+t_idx_name+' ON '+t_table+' ("'+str('","').join(l_fields)+'");'
                l_idx=[t_table,t_idx_name,t_idx_def]
                self.l_idx_def.append(l_idx)
            
        
    def build_pkeys_ddl(self):
        """ the function iterates over the list l_pkeys and builds a new list with the statements for pkeys """
        for l_pkey in self.l_pkeys:
            l_key=[]
            t_table=l_pkey[0]
            d_pkey=l_pkey[1]
            l_fields=d_pkey["constrained_columns"]
            if len(l_fields)>0:
                t_pkey_name='pk_'+t_table+'_'+str('_').join(l_fields)
                t_pkey_def='ALTER TABLE "'+t_table+'" ADD CONSTRAINT "'+t_pkey_name+'" PRIMARY KEY ("'+str('","').join(l_fields)+'") ;'
                l_key=[t_table,t_pkey_name,t_pkey_def]
                self.l_pkeys_def.append(l_key)
        
    def build_tab_ddl(self):
        """ the function iterates over the list l_tables and builds a new list with the statements for tables"""
        for l_table in self.l_tables:
            print "building the ddl for table "+l_table[0]
            t_head="CREATE TABLE "+'"'+l_table[0]+'" ('
            t_tail=");"
            l_body=[]
            l_columns=l_table[1]
            for l_column in l_columns:
                t_col_name='"'+l_column[0]+'"' 
                t_col_type=l_column[1]
                t_col_null=l_column[2]
                t_col_sec=l_column[3]
                if t_col_sec=='AUTOINC':
                    t_col_type='serial'
                
                l_body.append(t_col_name+ ' ' + t_col_type +' '+ t_col_null)
            t_body=str(',').join(l_body)
            l_table_def=[l_table[0],t_head+t_body+t_tail]
            self.l_tables_def.append(l_table_def)
            
            
class pg_data_flow:
    """class for managing the data flow into postgresql """
    def __init__(self,l_args):
        self.t_conf_file = l_args[0]
        self.l_tab_file = l_args[1]
        self.dic_conn={}
        self.load_conf()
        self.build_conn_string()
        self.connect_db()
        
    
    def connect_db(self):
        """ connect to the database using psycopy2 """
        self.o_pg_conn=psycopg2.connect(self.t_conn_str)
        self.o_pg_cur=self.o_pg_conn.cursor()
        
        
    def load_conf(self,):
        t_conf_file=self.t_conf_file
        if t_conf_file=='':
            t_cwd=os.getcwd()
            t_conf_file=t_cwd+'/config/pg_connection.conf'
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
        """builds the PostgreSQL connection string"""
        dic_conn=self.dic_conn
        self.t_conn_str="dbname="+dic_conn["dbname"]+" user="+dic_conn["dbuser"]+" host="+dic_conn["dbhost"]+" password="+dic_conn["dbpass"]+" port="+dic_conn["dbport"]
   
    def push_data(self):
        """ function to pull the data in copy format"""
        i_sequence=0
        for l_table in self.l_tab_file:
            print " starting import for table "+l_table[0]+" from file "+l_table[1]
            t_sql_copy="COPY "+'"'+l_table[0]+'"'+" FROM STDIN WITH NULL 'NULL' CSV QUOTE '\"' DELIMITER',' ESCAPE '\"'  "
            #print t_sql_copy
            o_file=open(l_table[1],'rb')
            #self.o_pg_cur.copy_from(o_file, '"'+l_table[0]+'"', sep=',')
            self.o_pg_cur.copy_expert(t_sql_copy,o_file)
            o_file.close()
            self.o_pg_conn.commit()
            print "import successful, removing the file "+l_table[1]
            os.remove(l_table[1])
            
            
            
    
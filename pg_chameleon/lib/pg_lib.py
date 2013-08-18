class pg_data_def:
    """class for building the postgresql ddl statements """
    def __init__(self,l_args):
        """ init function accept the objects initiated in the my_db_connection class  """
        self.l_tables=l_args[0]
        self.l_pkeys=l_args[1]
        self.l_table_def=[]
        self.l_pkeys_def=[]
        self.build_tab_ddl()
        self.build_pkeys_ddl()
        
    def save_ddl(self,t_file_name):
        """the function write the table and primary keys ddl in a file"""
        f_sql=open(t_file_name,'wb')
        for t_table_def in self.l_table_def:
            f_sql.write(t_table_def+"\n\n")
            
        for t_pkey in self.l_pkeys_def:
            f_sql.write(t_pkey+"\n\n")
        f_sql.close()
        
    def build_pkeys_ddl(self):
        """ the function iterates over the list l_pkeys and builds a new list with the statements for pkeys """
        for l_pkey in self.l_pkeys:
            t_table=l_pkey[0]
            d_pkey=l_pkey[1]
            l_fields=d_pkey["constrained_columns"]
            t_pkey_name='pk_'+t_table+'_'+str('_').join(l_fields)
            t_pkey_def='ALTER TABLE "'+t_table+'" ADD CONSTRAINT "'+t_pkey_name+'" PRIMARY KEY ("'+str('","').join(l_fields)+'") ;'
            self.l_pkeys_def.append(t_pkey_def)
        
    def build_tab_ddl(self):
        """ the function iterates over the list l_tables and builds a new list with the statements for tables"""
        for l_table in self.l_tables:
            print "building the dll for table "+l_table[0]
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
            self.l_table_def.append(t_head+t_body+t_tail)
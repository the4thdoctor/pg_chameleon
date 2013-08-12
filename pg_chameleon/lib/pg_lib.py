class pg_data_def:
    """class for building the postgresql ddl statements """
    def __init__(self,l_tables):
        """ init function accept the objects initiated in the my_db_connection class  """
        self.l_tables=l_tables
        self.build_dll()
    
    def build_dll(self):
        """ the function iterates over the list l_tables and build a new list with the statements """
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
            print t_head+t_body+t_tail
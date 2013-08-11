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
            t_ddl="CREATE TABLE "+'"'+l_table[0]+'"'
            l_columns=l_table[1]
            for l_column in l_columns:
                t_col_name=l_column[0] 
                print l_column[1] 
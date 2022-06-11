import re

class sql_parse(object):
    """
    The class tokenises the sql statements captured by mysql_engine.
    Several regular expressions analyse and build the elements of the token.
    The DDL support is purposely limited to the following.

    DROP PRIMARY KEY
    CREATE (UNIQUE) INDEX/KEY
    CREATE TABLE
    ALTER TABLE

    The regular expression m_fkeys is used to remove any foreign key definition from the sql statement
    as we don't enforce any foreign key on the PostgreSQL replication.
    """
    def __init__(self):
        """
            Class constructor the regular expressions are compiled and the token lists are initialised.
        """
        self.tokenised = []
        self.query_list = []
        self.pkey_cols = []
        self.ukey_cols = []
        
        self.reg_type = re.compile(r'(CREATE\s*?TABLE\s?|ALTER\s*?TABLE\s?|RENAME\s?TABLE\s?)', re.IGNORECASE)

    def clean_comments(self,statement):
        """
        """
        stat_cleanup=re.sub(r'/\*.*?\*/', '', statement, re.DOTALL)
        stat_cleanup=re.sub(r'--.*?\n', '', stat_cleanup)


    def parse_sql(self, sql_string):
        """
            Parses the sql string
            A regular expression replaces all the default value definitions with a space.
            Then the statements are split in a list using the statement separator;

            For each statement a set of regular expressions remove the comments, single and multi line.
            Parenthesis are surrounded by spaces and commas are rewritten in order to get at least one space after the comma.
            The statement is then put on a single line and stripped.

            Different match are performed on the statement.
            RENAME TABLE
            CREATE TABLE
            DROP TABLE
            ALTER TABLE
            ALTER INDEX
            DROP PRIMARY KEY
            TRUNCATE TABLE

            The match which is successful determines the parsing of the rest of the statement.
            Each parse builds a dictionary with at least two keys "name" and "command".

            Each statement parse comes with specific addictional keys.

            When the token dictionary is complete is added to the class list tokenised

            :param sql_string: The sql string with the sql statements.
        """
        statements=sql_string.split(';')
        for statement in statements:
            stat_dic={}
            statement_type = self.reg_type.match(statement.strip())
            if statement_type:
                print(statement_type.group(1))
            else:
                print(statement)
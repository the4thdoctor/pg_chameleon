import re

from parsy import alt, any_char, digit, eof, seq, string, success, regex, whitespace


def ci_string(s):
    """
    This function creates a case-insensitive parser for a string

    :param s: the string to make a case-insensitive parser for.
    :return: case-insensitive parser for string s
    :rtype: parsy.Parser
    """
    return string(s.upper(), transform=lambda x: x.upper()).result(s)


def optional_space_around(p):
    """
    This function extends an existing parser with optional whitespace
    around it. Whitespace is stripped from the parser's result.

    :param p: the parser to extend
    :return: a new parser with optional whitespace around it
    :rtype: parsy.Parser
    """
    return whitespace.optional() >> p << whitespace.optional()


pgsql_identifier = regex(r"\w+")
mysql_identifier = string("`") >> regex(r"[\w\s]+") << string("`")
identifier = pgsql_identifier | mysql_identifier

lparen = string("(")
rparen = string(")")
semicolon = string(";")
comma_sep = whitespace.optional() >> string(",") << whitespace.optional()
sql_string = (
    # single-quoted string
    (string("'") >> (string(r"\'") | any_char).until(string("'")).concat() << string("'")) |
    # double-quoted string
    (string('"') >> (string(r'\"') | any_char).until(string('"')).concat() << string('"'))
)

ci_word = regex(r"\w+", flags=re.IGNORECASE)

not_null = seq(ci_string("NOT"), whitespace, ci_string("NULL")).result("NOT NULL")
extra = ci_word.sep_by(whitespace)
ignored_by_column_def = alt(
    ci_string("UNIQUE"),
    seq(ci_string("PRIMARY"), whitespace, ci_string("KEY")).result("PRIMARY KEY"),
    ci_string("CONSTRAINT"),
    ci_string("INDEX"),
    seq(ci_string("FOREIGN"), whitespace, ci_string("KEY")).result("FOREIGN KEY")
)


pk_keyword = seq(ci_string("PRIMARY"), whitespace, ci_string("KEY"))

# special pkey definition which can be composite. Like:
# `CONSTRAINT pk_id PRIMARY KEY (id)` or `PRIMARY KEY (id1, id2)`
pk_definition = seq(
    __constraint=(ci_string("CONSTRAINT") >> whitespace >> identifier >> whitespace).optional(),
    index_name=pk_keyword.result("PRIMARY"),
    index_columns=whitespace.optional() >> lparen >> optional_space_around(identifier).sep_by(string(",")) << rparen,
    non_unique=success(0),
).combine_dict(dict)

# inline pk definition `COLUMN_NAME type other_inline_things PRIMARY KEY other_inline_things`
inline_pk_definition = seq(
    index_columns=identifier.map(lambda s: [s]),
    __extras=(whitespace >> regex(r"[\w_]+")).until(whitespace >> pk_keyword),
    index_name=(whitespace >> pk_keyword).result("PRIMARY"),
    __more_extras=(whitespace >> regex(r"[\w_]+")).many(),
    non_unique=success(0),
).combine_dict(dict)

# note: does not catch inline 'unique' 
# [CONSTRAINT uk_xyz] UNIQUE [KEY | INDEX] (column1, column2)
uk_definition = seq(
    __constraint=(ci_string("CONSTRAINT") >> whitespace >> identifier >> whitespace).optional(),
    index_name=ci_string("UNIQUE").result("UNIQUE"),
    __key_or_index_keyword=(whitespace >> (ci_string("INDEX") | ci_string("KEY"))).optional(),
    index_columns=whitespace.optional() >> lparen >> optional_space_around(identifier).sep_by(string(",")) << rparen,
    non_unique=success(0),
).combine_dict(dict)

idx_definition = seq(
    index_name=(ci_string("INDEX") | ci_string("KEY")).result("INDEX"),
    __optional_idx_name=(whitespace >> identifier).optional(),
    index_columns=whitespace.optional() >> lparen >> optional_space_around(identifier).sep_by(string(",")) << rparen,
    non_unique=success(1),
).combine_dict(dict)

fkey_definition = seq(
    __constraint=(ci_string("CONSTRAINT") >> whitespace >> identifier >> whitespace).optional(),
    index_name=(ci_string("FOREIGN") >> whitespace >> ci_string("KEY")).result("FOREIGN"),
    index_columns=whitespace.optional() >> lparen >> optional_space_around(identifier).sep_by(string(",")) << rparen,
    __references=whitespace.optional() >> ci_string("REFERENCES"),
    __other_table_name=whitespace.optional() >> identifier,
    __other_columns=whitespace.optional() >> lparen >> optional_space_around(identifier).sep_by(string(",")) << rparen,
    __on_delete_or_update=seq(
        whitespace, ci_string("ON"), whitespace, ci_string("UPDATE") | ci_string("DELETE"),
        whitespace, ci_string("CASCADE") | ci_string("RESTRICT"),
    ).many(),
    non_unique=success(1)
).combine_dict(dict)

other_key_definition = seq(
    __idx_type=((ci_string("UNIQUE") | ci_string("FULLTEXT")) << whitespace).optional(),
    index_name=(ci_string("INDEX") | ci_string("KEY")).result("OTHER"),
    __idx_name=whitespace >> identifier,
    index_columns=whitespace >> lparen >> optional_space_around(identifier).sep_by(string(",")) << rparen,
    non_unique=success(1),  # not correct, but it isn't used anywhere
).combine_dict(dict)

any_key_definition = alt(
    pk_definition,
    uk_definition,
    idx_definition,
    fkey_definition,
    other_key_definition
)

column_definition = seq(
    column_name=identifier,
    data_type=whitespace >> ci_word.map(lambda x: x.lower()),
    __precision_or_varying=(
        whitespace >>
        (ci_string("PRECISION") | ci_string("VARYING"))
    ).optional(),
    dimensions=(
        optional_space_around(lparen) >>
        digit.many().concat().sep_by(optional_space_around(string(",") | string("|")))
        << optional_space_around(rparen)
    ).optional(),
    enum_list=(
        optional_space_around(lparen) >>
        sql_string.sep_by(optional_space_around(string(",") | string("|")))
        << optional_space_around(rparen)
    ).optional(),
    extras=(
        whitespace >> alt(
            seq(ci_string("NOT"), whitespace, ci_string("NULL")).result("NOT NULL"),
            seq(ci_string("PRIMARY"), whitespace, ci_string("KEY")).result("PRIMARY KEY"),
            ci_word,
        ).sep_by(whitespace)
    ).optional(default=[]),
).combine_dict(dict)

create_table_statement = seq(
    command=(ci_string("CREATE") >> whitespace >> ci_string("TABLE")).result("CREATE TABLE"),
    __if_not_exists=seq(
        whitespace, ci_string("IF"),
        whitespace, ci_string("NOT"),
        whitespace, ci_string("EXISTS")
    ).optional(),
    name=whitespace >> identifier,
    inner=whitespace.optional() >> lparen >> (
        optional_space_around(
            any_key_definition.tag("index") | column_definition.tag("column")
        ).sep_by(string(","))
    ) << rparen,
    __rest=any_char.many(),
).combine_dict(dict)


class sql_token(object):
    """
    The class tokenises the sql statements captured by mysql_engine.
    Several regular expressions analyse and build the elements of the token.
    The DDL support is purposely limited to the following.

    DROP PRIMARY KEY
    CREATE (UNIQUE) INDEX/KEY
    CREATE TABLE
    ALTER TABLE

    The method post_process_key_definition ignores any foreign key definition from the
    sql statement as we don't enforce any foreign key on the PostgreSQL replication.
    """
    def __init__(self):
        """
            Class constructor the regular expressions are compiled and the token lists are initialised.
        """
        self.tokenised = []
        self.query_list = []
        self.pkey_cols = []
        self.ukey_cols = []

        #re for rename items
        self.m_rename_items = re.compile(r'(?:.*?\.)?(.*)\s*TO\s*(?:.*?\.)?(.*)(?:;)?', re.IGNORECASE)

        #re for fields
        self.m_dbl_dgt=re.compile(r'((\(\s?\d+\s?),(\s?\d+\s?\)))',re.IGNORECASE)
        self.m_dimension=re.compile(r'(\(.*?\))', re.IGNORECASE)

        #re for query type
        self.m_rename_table = re.compile(r'(RENAME\s*TABLE)\s*(.*)', re.IGNORECASE)
        self.m_alter_rename_table = re.compile(r'(?:(ALTER\s+?TABLE)\s+(`?\b.*?\b`?))\s+(?:RENAME)\s+(?:TO)?\s+(.*)', re.IGNORECASE)
        self.m_create_table = re.compile(r'(CREATE\s*TABLE)\s*(?:IF\s*NOT\s*EXISTS)?\s*(?:(?:`)?(?:\w*)(?:`)?\.)?(?:`)?(\w*)(?:`)?', re.IGNORECASE)
        self.m_drop_table = re.compile(r'(DROP\s*TABLE)\s*(?:IF\s*EXISTS)?\s*(?:`)?(\w*)(?:`)?', re.IGNORECASE)
        self.m_truncate_table = re.compile(r'(TRUNCATE)\s*(?:TABLE)?\s*(?:(?:`)?(\w*)(?:`)?)(?:.)?(?:`)?(\w*)(?:`)?', re.IGNORECASE)
        self.m_alter_index = re.compile(r'(?:(ALTER\s+?TABLE)\s+(`?\b.*?\b`?))\s+((?:ADD|DROP)\s+(?:UNIQUE)?\s*?(?:INDEX).*,?)', re.IGNORECASE)
        self.m_alter_table = re.compile(r'(?:(ALTER\s+?TABLE)\s+(?:`?\b.*?\b`\.?)?(`?\b.*?\b`?))\s+((?:ADD|DROP|CHANGE|MODIFY)\s+(?:\bCOLUMN\b)?.*,?)', re.IGNORECASE)
        self.m_alter_list = re.compile(r'((?:\b(?:ADD|DROP|CHANGE|MODIFY)\b\s+(?:\bCOLUMN\b)?))(.*?,)', re.IGNORECASE)
        self.m_alter_column = re.compile(r'\(?\s*`?(\w*)`?\s*(\w*(?:\s*\w*)?)\s*(?:\((.*?)\))?\)?', re.IGNORECASE)
        self.m_default_value = re.compile(r"(\bDEFAULT\b)\s*('?\w*'?)\s*", re.IGNORECASE)
        self.m_alter_change = re.compile(r'\s*`?(\w*)`?\s*`?(\w*)`?\s*(\w*)\s*(?:\((.*?)\))?', re.IGNORECASE)
        self.m_drop_primary = re.compile(r'(?:(?:ALTER\s+?TABLE)\s+(`?\b.*?\b`?)\s+(DROP\s+PRIMARY\s+KEY))', re.IGNORECASE)
        #self.m_modify = re.compile(r'((?:(?:ADD|DROP|CHANGE|MODIFY)\s+(?:\bCOLUMN\b)?))(.*?,)', re.IGNORECASE)
        self.m_ignore_keywords = re.compile(r'(CONSTRAINT)|(PRIMARY)|(INDEX)|(KEY)|(UNIQUE)|(FOREIGN\s*KEY)', re.IGNORECASE)

    def reset_lists(self):
        """
            The method resets the lists to empty lists after a successful tokenisation.
        """
        self.tokenised=[]
        self.query_list=[]

    def quote_cols(self, cols):
        """
            The method adds the " quotes to the column names.
            The string is converted to a list using the split method with the comma separator.
            The columns are then stripped and quoted with the "".
            Finally the list elements are rejoined in a string which is returned.
            The method is used in build_key_dic to sanitise the column names.

            :param cols: The columns string
            :return: The columns quoted between ".
            :rtype: text
        """
        idx_cols = cols.split(',')
        idx_cols = ['"%s"' % col.strip() for col in idx_cols]
        quoted_cols = ",".join(idx_cols)
        return quoted_cols

    def _post_process_key_definition(
        self, index_name, index_columns, non_unique, table_name, idx_counter
    ):
        """
            This function builds a new key_dic by overwriting the index_name if necessary and by
            discarding indices that are to be ignored (foreign key, fulltext, etc.). This discarding
            is done by returning a None value instead of the key_dic.

            ```
            key_dic format:
                index_name: str
                index_columns: list[str]
                non_unique: int 0|1
            ```

            :param index_name: The kind of index. One of PRIMARY, UNIQUE, INDEX, FOREIGN, OTHER
            :param index_columns: The columns covered by this index
            :param non_unique: Whether this index must enforce unique check or not
            :param table_name: The name of the table that is used to create a new index name
            :param idx_counter: An index counter that is used to create a new index name
            :return: The transformed key dic or None
            :rtype: dictionary | None
        """
        if index_name in {"FOREIGN", "OTHER"}:
            return None
        elif index_name == "PRIMARY":
            return dict(index_name="PRIMARY", index_columns=index_columns, non_unique=0)
        elif index_name == "UNIQUE":
            return dict(
                index_name=f"ukidx_{table_name[0:20]}_{idx_counter}",
                index_columns=index_columns,
                non_unique=0,
            )
        elif index_name == "INDEX":
            return dict(
                index_name=f"idx_{table_name[0:20]}_{idx_counter}",
                index_columns=index_columns,
                non_unique=1,
            )
        else:
            raise Exception(f"Unknown index name: {index_name}")

    def _post_process_column_definition(
            self, column_name, data_type, dimensions, enum_list, extras
    ):
        """
            This function does uses the parts identified by the column_definition parser
            and builds a dictionary in the col_dic format. It adds fields that are not identified
            directly by the parser.

            ```
            col_dic format:
              column_name: str
              data_type: str
              is_nullable: enum "YES"|"NO"
              enum_list: maybe str
              character_maximum_length: maybe str
              numeric_precision: maybe str
              numeric_scale: maybe str | int (defaults to 0)
              extra: str
              column_type: str
            ```

            The arguments accepted are the ones parsed by the column_definition parser.

            :param column_name: column_name as parsed by the column definition parser
            :param data_type: data_type as parsed by the column definition parser
            :param dimensions: the numeric dimensions defined as part of the column type
            :param enum_list: the enum list defined as part of the column type
            :param extras: other modifiers to the column
            :return: column dictionary which conforms to the col_dic format
            :rtype: dictionary
        """
        col_dict = dict(column_name=column_name, data_type=data_type)

        col_dict["is_nullable"] = "NO" if "NOT NULL" in extras else "YES"
        col_dict["extra"] = "auto_increment" if "AUTO_INCREMENT" in extras else ""

        if dimensions:
            col_dict["numeric_precision"] = col_dict["character_maximum_length"] = dimensions[0] # str
            col_dict["numeric_scale"] = dimensions[1] if len(dimensions) > 1 else 0 # str or int
        elif enum_list:
            col_dict["enum_list"] = "( %s )" % ", ".join(map(lambda x: f"'{x}'", enum_list))

        if dimensions:
            col_dict["column_type"] = "%s(%s)" % (data_type, ", ".join(dimensions))
        elif enum_list:
            col_dict["column_type"] = "%s(%s)" % (data_type, ", ".join(map(lambda x: f"'{x}'", enum_list)))
        else:
            col_dict["column_type"] = data_type

        return col_dict

    def parse_create_table(self, sql_create, table_name):
        """
            The method parse and generates a dictionary from the CREATE TABLE statement.

            The part of statement inside round brackets is parsed for column and index
            definitions. Index and column definitions are separated and processed one by one.
            First, indices are processed and added to self.pkey_cols and self.ukey_cols.
            Then columns are parsed and pkey_cols is modified if an inline primary key had been
            set.

            The indices are stored in the dictionary key "indices" as a list of dictionaries. Each
            key_dic has a fixed set of keys, as returned by _post_process_key_definition.

            The columns are stored in the dictionary key "columns" as a list of dictionaries. Each
            col_dic has a fixed set of keys, as returned by _post_process_column_definition.

            :param sql_create: The sql string with the CREATE TABLE statement
            :param table_name: The table name
            :return: table_dic the table dictionary tokenised from the CREATE TABLE
            :rtype: dictionary
        """

        table_dic = create_table_statement.parse(sql_create)
        columns_and_indices = table_dic.pop("inner")

        columns, indices = [], []
        for col_or_idx in columns_and_indices:
            tag, value = col_or_idx
            if tag == "column":
                columns.append(value)
            elif tag == "index":
                indices.append(value)
            else:
                raise Exception(f"unknown tag: {tag}")

        table_dic["columns"], table_dic["indices"] = [], []

        # post-process indices
        for raw_key_dic in indices:
            key_dic = self._post_process_key_definition(
                **raw_key_dic,
                table_name=table_dic["name"],
                idx_counter=len(table_dic["indices"])
            )
            if key_dic:
                table_dic["indices"].append(key_dic)

                # update self.pkey_cols or self.ukey_cols
                if key_dic["index_name"] == "PRIMARY":
                    self.pkey_cols = list(key_dic["index_columns"])
                elif raw_key_dic["index_name"] == "UNIQUE":
                    self.ukey_cols += [col_name for col_name in key_dic["index_columns"]]

        # post-process columns
        for raw_col_dic in columns:
            col_dic = self._post_process_column_definition(**raw_col_dic)
            if col_dic:
                # check for inline primary key definition
                if "PRIMARY KEY" in raw_col_dic["extras"]:
                    table_dic["indices"].append(dict(
                        index_name="PRIMARY", index_columns=[col_dic["column_name"]], non_unique=0
                    ))
                    self.pkey_cols = [col_dic["column_name"]]

                # must be non-nullable if column is ukey or pkey
                if (col_dic["column_name"] in self.pkey_cols or
                        col_dic["column_name"] in self.ukey_cols):
                    col_dic["is_nullable"] = "NO"

                table_dic["columns"].append(col_dic)

        return table_dic

    def parse_alter_table(self, malter_table):
        """
            The method parses the alter table match.
            As alter table can be composed of multiple commands the original statement (group 0 of the match object)
            is searched with the regexp m_alter_list.
            For each element in returned by findall the first word is evaluated as command. The parse alter table
            manages the following commands.
            DROP,ADD,CHANGE,MODIFY.

            Each command build a dictionary alter_dic with at leaset the keys command and name defined.
            Those keys are respectively the commant itself and the attribute name affected by the command.

            ADD defines the keys type and dimension. If type is enum then the dimension key stores the enumeration list.

            CHANGE defines the key command and then runs a match with m_alter_change. If the match is successful
            the following keys are defined.

            old is the old previous field name
            new is the new field name
            type is the new data type
            dimension the field's dimensions or the enum list if type is enum

            MODIFY works similarly to CHANGE except that the field is not renamed.
            In that case we have only the keys type and dimension defined along with name and command.s

            The class's regular expression self.m_ignore_keywords is used to skip the CONSTRAINT,INDEX and PRIMARY and FOREIGN KEY KEYWORDS in the
            alter command.

            :param malter_table: The match object returned by the match method against tha alter table statement.
            :return: stat_dic the alter table dictionary tokenised from the match object.
            :rtype: dictionary
        """
        stat_dic={}
        alter_cmd=[]
        alter_stat=malter_table.group(0) + ','
        stat_dic["command"]=malter_table.group(1).upper().strip()
        stat_dic["name"]=malter_table.group(2).strip().strip('`')
        dim_groups=self.m_dimension.findall(alter_stat)

        for dim_group in dim_groups:
            alter_stat=alter_stat.replace(dim_group, dim_group.replace(',','|'))

        alter_list=self.m_alter_list.findall(alter_stat)
        for alter_item in alter_list:
            alter_dic={}
            m_ignore_item = self.m_ignore_keywords.search(alter_item[1])

            if not m_ignore_item:
                command = (alter_item[0].split())[0].upper().strip()
                if command == 'DROP':
                    alter_dic["command"] = command
                    alter_dic["name"] = alter_item[1].strip().strip(',').replace('`', '').strip()
                elif command == 'ADD':
                    alter_string = alter_item[1].strip()

                    alter_column=self.m_alter_column.search(alter_string)
                    default_value = self.m_default_value.search(alter_string)
                    if alter_column:

                        column_type = alter_column.group(2).lower().strip()

                        alter_dic["command"] = command
                        # this is a lesser horrible hack, still needs to be improved
                        alter_dic["name"] = alter_column.group(1).strip().strip('`')
                        alter_dic["type"] = column_type.split(' ')[0]
                        try:
                            alter_dic["dimension"]=alter_column.group(3).replace('|', ',').strip()
                        except:
                            alter_dic["dimension"]=0
                        if default_value:
                            alter_dic["default"] = default_value.group(2)
                        else:
                            alter_dic["default"] = None

                elif command == 'CHANGE':
                    alter_dic["command"] = command
                    alter_column = self.m_alter_change.search(alter_item[1].strip())
                    if alter_column:
                        alter_dic["command"] = command
                        alter_dic["old"] = alter_column.group(1).strip().strip('`')
                        alter_dic["new"] = alter_column.group(2).strip().strip('`')
                        alter_dic["type"] = alter_column.group(3).strip().strip('`').lower()
                        alter_dic["name"] = alter_column.group(1).strip().strip('`')
                        try:
                            alter_dic["dimension"]=alter_column.group(4).replace('|', ',').strip()
                        except:
                            alter_dic["dimension"]=0

                elif command == 'MODIFY':
                    alter_string = alter_item[1].strip()

                    alter_column = self.m_alter_column.search(alter_string)
                    if alter_column:
                        alter_dic["command"] = command
                        alter_dic["name"] = alter_column.group(1).strip().strip('`')
                        # this is a lesser horrible hack, still needs to be improved
                        column_type = alter_column.group(2).lower().strip()
                        alter_dic["type"] = column_type.split(' ')[0]
                        try:
                            alter_dic["dimension"]=alter_column.group(3).replace('|', ',').strip()
                        except:
                            alter_dic["dimension"] = 0
                if command != 'DROP':
                    alter_dic["data_type"] = alter_dic["type"]
                    if alter_dic["dimension"] == 0:
                        alter_dic["column_type"] = alter_dic["type"]
                    else:
                        alter_dic["column_type"] = "%s(%s)" % (alter_dic["type"], alter_dic["dimension"])
                alter_cmd.append(alter_dic)
            stat_dic["alter_cmd"]=alter_cmd
        return stat_dic

    def parse_rename_table(self, rename_statement):
        """
            The method parses the rename statements storing in a list the
            old and the new table name.

            :param rename_statement: The statement string without the RENAME TABLE
            :return: rename_list, a list with the old/new table names inside
            :rtype: list

        """
        rename_list = []
        for rename in rename_statement.split(','):
            mrename_items = self.m_rename_items.search(rename.strip())
            if mrename_items:
                rename_list.append([item.strip().replace('`', '') for item in mrename_items.groups()])
        return rename_list

    def parse_sql(self, sql_string):
        """
            The method cleans and parses the sql string
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
            stat_cleanup=re.sub(r'/\*.*?\*/', '', statement, re.DOTALL)
            stat_cleanup=re.sub(r'--.*?\n', '', stat_cleanup)
            stat_cleanup=re.sub(r'[\b)\b]', ' ) ', stat_cleanup)
            stat_cleanup=re.sub(r'[\b(\b]', ' ( ', stat_cleanup)
            stat_cleanup=re.sub(r'[\b,\b]', ', ', stat_cleanup)
            stat_cleanup=stat_cleanup.replace('\n', ' ')
            stat_cleanup = re.sub("\([\w*\s*]\)", " ",  stat_cleanup)
            stat_cleanup = stat_cleanup.strip()
            malter_rename = self.m_alter_rename_table.match(stat_cleanup)
            mrename_table = self.m_rename_table.match(stat_cleanup)
            mcreate_table = self.m_create_table.match(stat_cleanup)
            mdrop_table = self.m_drop_table.match(stat_cleanup)
            malter_table = self.m_alter_table.match(stat_cleanup)
            malter_index = self.m_alter_index.match(stat_cleanup)
            mdrop_primary = self.m_drop_primary.match(stat_cleanup)
            mtruncate_table = self.m_truncate_table.match(stat_cleanup)
            if malter_rename:
                stat_dic["command"] = "RENAME TABLE"
                stat_dic["name"] = malter_rename.group(2)
                stat_dic["new_name"] = malter_rename.group(3)
                self.tokenised.append(stat_dic)
                stat_dic = {}
            elif mrename_table:
                rename_list = self.parse_rename_table(mrename_table.group(2))
                for rename_table in rename_list:
                    stat_dic["command"] = "RENAME TABLE"
                    stat_dic["name"] = rename_table[0]
                    stat_dic["new_name"] = rename_table[1]
                    self.tokenised.append(stat_dic)
                    stat_dic = {}

            elif mcreate_table:
                command=' '.join(mcreate_table.group(1).split()).upper().strip()
                stat_dic["command"]=command
                stat_dic["name"]=mcreate_table.group(2)
                create_parsed=self.parse_create_table(stat_cleanup, stat_dic["name"])
                stat_dic["columns"]=create_parsed["columns"]
                stat_dic["indices"]=create_parsed["indices"]
            elif mdrop_table:
                command=' '.join(mdrop_table.group(1).split()).upper().strip()
                stat_dic["command"]=command
                stat_dic["name"]=mdrop_table.group(2)
            elif mtruncate_table:
                command=' '.join(mtruncate_table.group(1).split()).upper().strip()
                stat_dic["command"]=command
                if mtruncate_table.group(3) == '':
                    stat_dic["name"]=mtruncate_table.group(2)
                else:
                    stat_dic["name"]=mtruncate_table.group(3)
            elif mdrop_primary:
                stat_dic["command"]="DROP PRIMARY KEY"
                stat_dic["name"]=mdrop_primary.group(1).strip().strip(',').replace('`', '').strip()
            elif malter_index:
                pass
            elif malter_table:
                stat_dic=self.parse_alter_table(malter_table)
                if len(stat_dic["alter_cmd"]) == 0:
                    stat_dic = {}

            if stat_dic!={}:
                self.tokenised.append(stat_dic)



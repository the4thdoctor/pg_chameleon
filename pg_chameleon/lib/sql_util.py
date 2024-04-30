import re

from parsy import alt, any_char, digit, eof, forward_declaration, seq, string, success, regex, whitespace


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


def parentheses_around(p):
    """
    This function extends an existing parser with parentheses
    around it. The captured parentheses and adjacent whitespace
    are discarded.

    :param p: the parser to extend
    :return: a new parser with parenthesis around the old parser
    :rtype: parsy.Parser
    """
    return optional_space_around(lparen) >> p << optional_space_around(rparen)


pgsql_identifier = regex(r"\w+")
mysql_identifier = string("`") >> regex(r"[\w\s]+") << string("`")
identifier = pgsql_identifier | mysql_identifier
number = digit.many().concat()

lparen = string("(")
rparen = string(")")
semicolon = string(";")
comma_sep = optional_space_around(string(","))
sql_string = (
    # single-quoted string
    (string("'") >> (string(r"\'") | any_char).until(string("'")).concat() << string("'")) |
    # double-quoted string
    (string('"') >> (string(r'\"') | any_char).until(string('"')).concat() << string('"'))
)

ci_word = regex(r"\w+")

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

    # for parsing complex index definitions
    # forward_declaration is like lazy initialisation
    inline_expr = forward_declaration()
    simple_expr = regex(r"[^\(\)]+")
    group_expr = seq(optional_space_around(lparen), inline_expr.many().concat(), optional_space_around(rparen)).concat()
    inline_expr.become(group_expr | simple_expr)

    # { column_name | column_name(n) | (FUNCTION(column_name)) }
    key_part = alt(
        # column_name(80)
        seq(
            identifier,
            optional_space_around(lparen), number, optional_space_around(rparen),
        ).concat().tag("partial"),

        # column_name
        identifier.tag("column"),

        # functional
        inline_expr.tag("functional"),
    )

    key_part_group = key_part.sep_by(comma_sep).map(
        lambda tagged_list: {
            "tags": [tag for (tag, _value) in tagged_list],
            "values": [value for (_tag, value) in tagged_list],
        }
    ).combine_dict(
        lambda tags, values: {
            "is_functional": "functional" in tags,
            "is_partial": "partial" in tags,
            "index_columns": values,
        }
    )

    # [CONSTRAINT pk_id] PRIMARY KEY (column_name, ...)
    pk_definition = seq(
        __constraint=(ci_string("CONSTRAINT") >> whitespace >> identifier >> whitespace).optional(),
        index_name=seq(ci_string("PRIMARY"), whitespace, ci_string("KEY")).result("PRIMARY"),
        index_columns=parentheses_around(key_part.sep_by(comma_sep)),
        non_unique=success(0),
        is_fulltext=success(False),
        is_spatial=success(False),
    ).combine_dict(dict)

    # [CONSTRAINT uk_xyz] UNIQUE [{KEY | INDEX}] (column_name, ...)
    uk_definition = seq(
        __constraint=(ci_string("CONSTRAINT") >> whitespace >> identifier >> whitespace).optional(),
        index_name=ci_string("UNIQUE").result("UNIQUE"),
        __key_or_index_keyword=(whitespace >> (ci_string("INDEX") | ci_string("KEY"))).optional(),
        __optional_idx_name=(whitespace >> identifier).optional(),
        index_type=(
            whitespace >> ci_string("USING") >> (ci_string("BTREE") | ci_string("HASH"))
        ).optional(),
        index_columns=parentheses_around(key_part.sep_by(comma_sep)),
        non_unique=success(0),
        is_fulltext=success(False),
        is_spatial=success(False),
    ).combine_dict(dict)

    # {INDEX | KEY} [idx_name] (column_name, ...)
    idx_definition = seq(
        index_name=(ci_string("INDEX") | ci_string("KEY")).result("INDEX"),
        __optional_idx_name=(whitespace >> identifier).optional(),
        index_type=(
            whitespace >> ci_string("USING") >> (ci_string("BTREE") | ci_string("HASH"))
        ).optional(),
        index_columns=parentheses_around(key_part.sep_by(comma_sep)),
        non_unique=success(1),
        is_fulltext=success(False),
        is_spatial=success(False),
    )

    # [CONSTRAINT fk_id] FOREIGN KEY REFERENCES table_name (column_name, ...)
    # [ON {UPDATE | DELETE} {CASCADE | RESTRICT}]
    fkey_definition = seq(
        __constraint=(ci_string("CONSTRAINT") >> whitespace >> identifier >> whitespace).optional(),
        index_name=(ci_string("FOREIGN") >> whitespace >> ci_string("KEY")).result("FOREIGN"),
        index_columns=parentheses_around(key_part.sep_by(comma_sep)),
        __references=whitespace.optional() >> ci_string("REFERENCES"),
        __other_table_name=whitespace.optional() >> identifier,
        __other_columns=parentheses_around(key_part.sep_by(comma_sep)),
        __on_delete_or_update=seq(
            whitespace, ci_string("ON"), whitespace, ci_string("UPDATE") | ci_string("DELETE"),
            whitespace, ci_string("CASCADE") | ci_string("RESTRICT"),
        ).many(),
        non_unique=success(1),
        is_fulltext=success(False),
        is_spatial=success(False),
    ).combine_dict(dict)

    # {SPATIAL | FULLTEXT} {INDEX | KEY} idx_name (column_name, ...)
    other_key_definition = seq(
        __constraint=(ci_string("CONSTRAINT") >> whitespace >> identifier >> whitespace).optional(),
        index_name=whitespace.optional() >> (ci_string("SPATIAL") | ci_string("FULLTEXT")),
        __index_or_key=(whitespace >> (ci_string("INDEX") | ci_string("KEY"))).optional(),
        __idx_name=whitespace >> identifier,
        index_columns=parentheses_around(key_part.sep_by(comma_sep)),
        non_unique=success(1),
    ).combine_dict(
        lambda index_name, **rest: {
            "is_spatial": index_name.upper() == "SPATIAL",
            "is_fulltext": index_name.upper() == "FULLTEXT",
            "index_name": "OTHER",
            **rest,
        }
    )

    any_key_definition = alt(
        pk_definition,
        uk_definition,
        idx_definition,
        fkey_definition,
        other_key_definition
    ).combine_dict(
        lambda index_columns, **kwargs: {
            "key_part_tags": [tag for (tag, _value) in index_columns],
            "index_columns": [value for (_tag, value) in index_columns],
            **kwargs,
        }
    ).combine_dict(
        lambda key_part_tags, **kwargs: {
            "is_functional": "functional" in key_part_tags,
            "is_partial": "partial" in key_part_tags,
            **kwargs,
        }
    )

    # column_name type [PRECISION | VARYING] [(numeric_dimension, ...)] [('enum_list', ...)]
    # [NOT NULL] [PRIMARY KEY] [DEFAULT {'string' | literal} ] [ignored extras ...]
    column_definition = seq(
        column_name=identifier,
        data_type=whitespace >> ci_word.map(lambda x: x.lower()),
        __precision_or_varying=(
            whitespace >>
            (ci_string("PRECISION") | ci_string("VARYING"))
        ).optional(),
        dimensions=(
            optional_space_around(lparen) >>
            number.sep_by(
                comma_sep | optional_space_around(string("|"))
            )
            << optional_space_around(rparen)
        ).optional(),
        enum_list=(
            optional_space_around(lparen) >>
            sql_string.sep_by(
                comma_sep | optional_space_around(string("|"))
            )
            << optional_space_around(rparen)
        ).optional(),
        extras=(
            whitespace.optional() >> alt(
                seq(ci_string("NOT"), whitespace, ci_string("NULL")).result("NOT NULL"),
                seq(ci_string("PRIMARY"), whitespace, ci_string("KEY")).result("PRIMARY KEY"),
                seq(
                    ci_string("DEFAULT").result("DEFAULT"),
                    whitespace >> (sql_string.map(lambda x: f"'{x}'") | ci_word)
                ),
                seq(ci_string("COMMENT"), whitespace, sql_string).result("COMMENT"),
                identifier,
                ci_word,
            ).sep_by(whitespace)
        ).optional(default=[]),
    ).combine_dict(dict)

    # CREATE TABLE [IF NOT EXISTS] table_name (key_definition | column_definition, ...) [anything extra]
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

    # ALTER TABLE table_name RENAME [TO] new_name
    alter_rename_table_statement = seq(
        command=seq(ci_string("ALTER"), whitespace, ci_string("TABLE")).result("RENAME TABLE"),
        name=whitespace >> identifier,
        __rename=whitespace >> ci_string("RENAME"),
        __to=(whitespace >> ci_string("TO")).optional(),
        new_name=whitespace >> identifier,
    ).combine_dict(dict)

    # [schema_identifier.]table_name TO [new_schema_identifier.]new_name
    rename_table_item = seq(
        command=success("RENAME TABLE"),
        __from_schema=(identifier >> string(".")).optional(),
        name=identifier,
        __to=whitespace >> ci_string("TO"),
        __to_schema=whitespace >> (identifier >> string(".")).optional(),
        new_name=identifier,
    ).combine_dict(dict)

    # RENAME TABLE ( [old_schema.]old_name TO [new_schema.]new_name )
    # returns list, not dict!
    rename_table_statement = (
        ci_string("RENAME") >> whitespace >> ci_string("TABLE") >>
        optional_space_around(rename_table_item).sep_by(string(","))
    )

    # DROP TABLE [IF EXISTS] table_name
    drop_table_statement = seq(
        command=seq(ci_string("DROP"), whitespace, ci_string("TABLE")).result("DROP TABLE"),
        __if_exists=seq(whitespace, ci_string("IF"), whitespace, ci_string("EXISTS")).optional(),
        name=whitespace >> identifier,
    ).combine_dict(dict)

    # ALTER TABLE table_name DROP PRIMARY KEY
    drop_primary_key_statement = seq(
        command=seq(ci_string("ALTER"), whitespace, ci_string("TABLE")).result("DROP PRIMARY KEY"),
        name=whitespace >> identifier,
        __drop_pk=whitespace >> seq(
            ci_string("DROP"), whitespace, ci_string("PRIMARY"), whitespace, ci_string("KEY")
        )
    ).combine_dict(dict)

    # TRUNCATE [TABLE] [schema_name.]table_name
    truncate_table_statement = seq(
        command=ci_string("TRUNCATE"),
        __table=(whitespace >> ci_string("TABLE")).optional(),
        __space=whitespace,
        __schema=(identifier << string(".")).optional(),
        name=identifier,
    ).combine_dict(dict)

    # post processes a parsed column definition
    # when it occurs in an ALTER TABLE statement
    column_definition_in_alter_table = column_definition.combine_dict(
        lambda column_name, data_type, dimensions, enum_list, extras, **k: {
            "name": column_name,
            "type": data_type,
            "dimension": (
                ", ".join(dimensions) if dimensions else
                ", ".join(map(lambda x: f"'{x}'", enum_list)) if enum_list else
                0
            ),
            "default": (
                next(
                    (extra[1] for extra in extras
                    if isinstance(extra, list) and extra[0] == "DEFAULT"),
                    None
                )
            )
        }
    ).map(
        lambda d: dict(
            d,
            data_type=d["type"],
            column_type="%s(%s)" % (d["type"], d["dimension"]) if d["dimension"] else d["type"]
        )
    )

    # ADD [COLUMN] column_definition
    alter_table_add = seq(
        command=ci_string("ADD"),
        __column=(whitespace >> ci_string("COLUMN")).optional(),
        col_def=whitespace >> column_definition_in_alter_table
    ).combine_dict(
        lambda command, col_def: dict(command=command, **col_def)
    )

    # ADD [COLUMN] (column_definition, ...)
    alter_table_add_multiple = seq(
        command=ci_string("ADD").result("ADD MULTIPLE"),
        __column=(whitespace >> ci_string("COLUMN")).optional(),
        col_defs=optional_space_around(
            parentheses_around(
                column_definition_in_alter_table.sep_by(comma_sep)
            )
        ),
    ).combine_dict(dict)

    # CHANGE [COLUMN] column_name column_definition
    alter_table_change = seq(
        command=ci_string("CHANGE"),
        __column=(whitespace >> ci_string("COLUMN")).optional(),
        old=whitespace >> identifier,
        col_def=whitespace >> column_definition_in_alter_table,
    ).combine_dict(
        lambda command, old, col_def: dict(
            command=command,
            old=old,
            new=col_def["name"],
            **col_def,
        )
    ).combine_dict(dict)

    # DROP [COLUMN] column_name
    alter_table_drop = seq(
        command=ci_string("DROP"),
        __column=(whitespace >> ci_string("COLUMN")).optional(),
        name=whitespace >> identifier,
    ).combine_dict(dict)

    # MODIFY [COLUMN] column_definition
    alter_table_modify = seq(
        command=ci_string("MODIFY"),
        __column=(whitespace >> ci_string("COLUMN")).optional(),
        col_def=whitespace >> column_definition_in_alter_table,
    ).combine_dict(
        lambda command, col_def: dict(command=command, **col_def)
    )

    # ADD {INDEX | KEY} [index_name] [USING {BTREE | HASH}] (key_part, ...)
    alter_table_add_index = seq(
        command=ci_string("ADD").result("ADD INDEX"),
        key_dic=whitespace >> any_key_definition,
    ).combine_dict(
        lambda command, key_dic: dict(command=command, **key_dic)
    )

    # RENAME {INDEX | KEY} old_index_name TO new_index_name
    alter_table_rename_index = seq(
        command=seq(ci_string("RENAME"), whitespace, ci_string("INDEX") | ci_string("KEY")).result("RENAME INDEX"),
        index_name=whitespace >> identifier,
        __to=whitespace >> ci_string("TO"),
        new_index_name=whitespace >> identifier,
    ).combine_dict(dict)

    # DROP {INDEX | KEY} index_name
    alter_table_drop_index = seq(
        command=seq(ci_string("DROP"), whitespace, ci_string("INDEX") | ci_string("KEY")).result("DROP INDEX"),
        index_name=whitespace >> identifier,
    )

    # [ADD | DROP | CHANGE | MODIFY] {INDEX | KEY | CONSTRAINT | CHECK | UNIQUE | FOREIGN KEY | PRIMARY KEY}
    alter_table_ignored = seq(
        ci_string("ADD") | ci_string("DROP") | ci_string("CHANGE") | ci_string("MODIFY"),
        whitespace >> alt(
            ci_string("INDEX"), ci_string("KEY"), ci_string("CONSTRAINT"), ci_string("CHECK"),
            ci_string("UNIQUE"), seq(ci_string("FOREIGN"), whitespace, ci_string("KEY")),
            seq(ci_string("PRIMARY"), whitespace, ci_string("KEY"))
        ),
        any_char.until(string(",") | eof).concat(),
    ).result(None)

    # ALTER TABLE [schema_name.]table_name alter_table_subcommand, ...
    alter_table_statement = seq(
        command=seq(ci_string("ALTER"), whitespace, ci_string("TABLE")).result("ALTER TABLE"),
        __space=whitespace,
        __schema=seq(identifier, string(".")).optional(),
        name=identifier,
        alter_cmd=optional_space_around(
            alt(
                alter_table_add_index,
                alter_table_rename_index,
                alter_table_drop_index,
                alter_table_ignored,
                alter_table_drop,
                alter_table_add,
                alter_table_add_multiple,
                alter_table_change,
                alter_table_modify,
            ).sep_by(comma_sep, min=1).map(lambda ls: [x for x in ls if x])
        )
    ).combine_dict(dict)

    # CREATE [UNIQUE] INDEX index_name ON table_name (column_name, ...)
    create_index_statement = seq(
        command=ci_string("CREATE").result("ADD INDEX"),
        non_unique=(whitespace >> ci_string("UNIQUE")).result(0).optional(default=1),
        is_fulltext=(whitespace >> ci_string("FULLTEXT")).result(True).optional(default=False),
        is_spatial=(whitespace >> ci_string("SPATIAL")).result(True).optional(default=False),
        __index=whitespace >> ci_string("INDEX"),
        index_name=whitespace >> identifier,
        index_type=(
            optional_space_around(ci_string("USING")) >> (ci_string("BTREE") | ci_string("HASH"))
        ).optional(),
        __on=whitespace >> ci_string("ON"),
        __space=whitespace,
        __schema=seq(identifier, string(".")).optional(),
        name=identifier,
        key_parts=parentheses_around(key_part_group),
    ).combine_dict(
        lambda key_parts, **rest: {**key_parts, **rest}
    ).combine_dict(
        lambda command, name, **key_dic: {
            "command": "ALTER TABLE",
            "name": name,
            "alter_cmd": [{"command": command, **key_dic}],
        }
    )

    # DROP INDEX index_name ON table_name
    # equivalent to ALTER TABLE ... DROP INDEX ...
    drop_index_statement = seq(
        __drop_index=seq(ci_string("DROP"), whitespace, ci_string("INDEX")),
        index_name=whitespace >> identifier,
        __on=whitespace >> ci_string("ON"),
        name=whitespace >> identifier,
    ).combine_dict(
        lambda name, index_name: {
            "command": "ALTER TABLE",
            "name": name,
            "alter_cmd": [{"command": "DROP INDEX", "index_name": index_name}],
        }
    )

    def __init__(self):
        """
            Class constructor the regular expressions are compiled and the token lists are initialised.
        """
        self.tokenised = []
        self.query_list = []
        self.pkey_cols = []
        self.ukey_cols = []

        # supported statements
        # RENAME TABLE
        # CREATE TABLE
        # DROP TABLE
        # TRUNCATE TABLE
        # ALTER TABLE
        # CREATE INDEX
        # DROP INDEX
        self.sql_parser = optional_space_around(
            alt(
                self.rename_table_statement,
                self.create_table_statement.map(self._post_process_create_table),
                self.drop_table_statement,
                self.truncate_table_statement,
                self.drop_primary_key_statement,
                self.create_index_statement,
                self.drop_index_statement,
                self.alter_table_statement.map(self._post_process_alter_table),
                self.alter_rename_table_statement,
            ).optional()
        )

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
        self,
        index_name,
        index_columns,
        non_unique,
        table_name,
        idx_counter,
        **tags,
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
            :param tags: Flags such as is_functional, is_partial, is_fulltext, etc.
            :return: The transformed key dic or None
            :rtype: dictionary | None
        """
        if index_name in {"FOREIGN", "OTHER"}:
            return None
        elif index_name == "PRIMARY":
            return dict(
                index_name="PRIMARY",
                index_columns=index_columns,
                non_unique=0,
                **tags,
            )
        elif index_name == "UNIQUE":
            return dict(
                index_name=f"ukidx_{table_name[0:20]}_{idx_counter}",
                index_columns=index_columns,
                non_unique=0,
                **tags,
            )
        elif index_name == "INDEX":
            return dict(
                index_name=f"idx_{table_name[0:20]}_{idx_counter}",
                index_columns=index_columns,
                non_unique=1,
                **tags,
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

    def _post_process_create_table(self, table_dic):
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
            :return: table_dic the table dictionary tokenised from the CREATE TABLE
            :rtype: dictionary
        """

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

    def _post_process_alter_table(self, alter_table_dic):
        """
        Post process alter table statement's parsed result to make it
        compatible with the expected result type.
        """
        # make a copy of alter_cmd list so we can
        # modify the original list freely
        alter_cmds = list(alter_table_dic["alter_cmd"])

        for cmd in alter_cmds:
            if cmd["command"] == "ADD MULTIPLE":
                alter_table_dic["alter_cmd"].remove(cmd)
                alter_table_dic["alter_cmd"].extend([
                    dict(command="ADD", **col_def)
                    for col_def in cmd["col_defs"]
                ])
        return alter_table_dic

    def parse_sql(self, sql_string):
        """
            The method removes comments from the sql string and parses it.
            The statements are split in a list using the statement separator ;

            For each statement, the sql parser tries to parse it with
            a set of specific statement parsers. If any supported statement
            is found, it is parsed and returned as a non-empty dict or
            a list of dicts by the parser.

            Look at self.sql_parser for the different statements that are
            supported by the parser.

            Different match are performed on the statement.
            RENAME TABLE
            CREATE TABLE
            DROP TABLE
            ALTER TABLE
            ALTER INDEX
            DROP PRIMARY KEY
            TRUNCATE TABLE

            Each successful parse builds a dictionary with at least two
            keys "name" and "command".

            Each statement parse comes with specific addictional keys.

            When the token dictionary is complete is added to the class list tokenised

            :param sql_string: The sql string with the sql statements.
        """
        sql_string_cleanup = re.sub(r'/\*.*?\*/', '', sql_string, re.DOTALL)
        sql_string_cleanup = re.sub(r'--.*?\n', '', sql_string_cleanup)

        multiple_sql_parser = self.sql_parser.sep_by(optional_space_around(string(";")))
        for stat_dic in multiple_sql_parser.parse(sql_string_cleanup):
            if isinstance(stat_dic, dict) and stat_dic != {}:
                self.tokenised.append(stat_dic)
            elif isinstance(stat_dic, list):
                for d in stat_dic:
                    if isinstance(d, dict) and stat_dic != {}:
                        self.tokenised.append(stat_dic)

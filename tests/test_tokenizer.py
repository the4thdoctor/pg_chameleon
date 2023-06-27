import pytest


from pg_chameleon.lib.sql_util import sql_token


tokenizer = sql_token()


@pytest.mark.parametrize(
    "parser, string, expectation",
    [
        (
            tokenizer.column_definition,
            "id int PRIMARY KEY",
            dict(column_name="id", data_type="int",
                 dimensions=None, enum_list=None, extras=["PRIMARY KEY"])
        ),
        (
            tokenizer.any_key_definition,
            "CONSTRAINT pk_id PRIMARY KEY (id)",
            dict(index_name="PRIMARY", index_columns=["id"], non_unique=0)
        ),
        (
            tokenizer.any_key_definition,
            "CONSTRAINT pk_id1_id2 PRIMARY KEY (id1,id2)",
            dict(index_name="PRIMARY", index_columns=["id1", "id2"], non_unique=0)
        ),
        (
            tokenizer.any_key_definition,
            "PRIMARY\n KEY(id1, id2, `id3`, `id 4` )",
            dict(index_name="PRIMARY", index_columns=["id1", "id2", "id3", "id 4"], non_unique=0)
        ),
        (
            tokenizer.any_key_definition,
            "UNIQUE KEY (id1, id2)",
            dict(index_name="UNIQUE", index_columns=["id1", "id2"], non_unique=0)
        ),
        (
            tokenizer.any_key_definition,
            "UNIQUE KEY (id1)",
            dict(index_name="UNIQUE", index_columns=["id1"], non_unique=0)
        ),
        (
            tokenizer.any_key_definition,
            "UNIQUE (id1, id2)",
            dict(index_name="UNIQUE", index_columns=["id1", "id2"], non_unique=0)
        ),
        (
            tokenizer.any_key_definition,
            "KEY (id1)",
            dict(index_name="INDEX", index_columns=["id1"], non_unique=1)
        ),
        (
            tokenizer.any_key_definition,
            "FULLTEXT KEY asdf (id)",
            dict(index_name="OTHER", index_columns=["id"], non_unique=1),
        ),
        (
            tokenizer.any_key_definition,
            "FOREIGN KEY (column1) REFERENCES table2 (column2)",
            dict(index_name="FOREIGN", index_columns=["column1"], non_unique=1)
        ),
        (
            tokenizer.column_definition,
            """film_rating smallint not null
            DEFAULT 3""",
            dict(column_name="film_rating", data_type="smallint",
                 dimensions=None, enum_list=None,
                 extras=["NOT NULL", ["DEFAULT", "3"]])
        ),
        (
            tokenizer.column_definition,
            """film_rating
            ENUM ('1 star', '2 star', '3 star', '4 star', '5 star')
            DEFAULT '3 star'""",
            dict(column_name="film_rating", data_type="enum",
                 dimensions=None, enum_list=["1 star", "2 star", "3 star", "4 star", "5 star"],
                 extras=[["DEFAULT", "'3 star'"]])
        )
    ]
)
def test_parses_successfully(parser, string, expectation):
    output = parser.parse(string)
    assert output == expectation

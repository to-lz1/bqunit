from bqunit.query_builder import QueryBuilder


def test_extract_table_names():
    query = """
    select
      tbl1.col1
      , tbl2.col2
      , tbl1.col2
      , tbl1.col3
    from
      `prj_12345.data_set.table` tbl1
      join (
        select
          col1, 'test' as col2
        from
          another_data_set.another_table_*
        where
          _TABLE_SUFFIX >= '20200101'
      ) tbl2
        on tbl1.col1 = tbl2.col1
    """
    extracted = QueryBuilder.extract_table_names(query)
    assert 'prj_12345.data_set.table' in extracted
    assert 'another_data_set.another_table_' in extracted


def test_replace_by_dict():
    builder = QueryBuilder(dataset_name='bqunit')
    dic = {
        'source.table1': 't_1b87',
        'source.table2': 't_fc28',
    }

    query_1 = 'select col1, col2 from foo.bar'
    replaced1 = builder.replace_by_dict(query_1, dic)
    assert query_1 == replaced1

    query_2 = 'select col1, from `source.table1` union all select col1 from `source.table2`'
    replaced2 = builder.replace_by_dict(query_2, dic)
    assert replaced2 == 'select col1, from `bqunit.t_1b87` union all select col1 from `bqunit.t_fc28`'

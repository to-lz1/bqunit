from bqunit.table_mapper import TableMapper


def test_table_mapper():
    mapper = TableMapper()
    mapper.register('test_table_1')
    mapper.register('dataset.test_table_2')

    assert len(mapper.registered_tables()) == 2
    assert 'test_table_1' in mapper.registered_tables()
    assert 'test_table_2' not in mapper.registered_tables()
    assert 'dataset.test_table_2' in mapper.registered_tables()

    assert len(mapper.redirected_tables()) == 2
    assert mapper.get('test_table_1') != mapper.get('dataset.test_table_2')
    assert mapper.get('test_table_1') in mapper.redirected_tables()

    dic = mapper.as_dict()
    assert len(dic) == 2
    assert mapper.get('test_table_1') == dic['test_table_1']

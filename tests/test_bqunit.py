import pytest

from bqunit.bqunit import BQUnit


def test_general_use_case():
    # User have to prepare BQ project, and dataset for test bed.
    # If Application Default Credential is set, project id is not required.
    bqunit = BQUnit(dataset_name='bqunit')
    bqunit.fixture(
        table_name='dummy-project-123456.dummy_dataset.dummy_table',
        statement="""
        select 1 as col1, 'str_1' as col2, true as col3
        union all
        select 2, 'str_2', false
        """)
    tested_query = """
       select col1, col2
       from `dummy-project-123456.dummy_dataset.dummy_table` foo
       where col1 = 1
     """
    query_result = bqunit.test_query(tested_query)
    assert query_result.total_rows == 1
    bqunit.teardown()  # drop all temporary tables.


def test_fixture_validation():
    bqunit = BQUnit(dataset_name='bqunit')
    tested_query = """
       select col1, col2
       from `dummy-project-123456.dummy_dataset.dummy_table` foo
     """
    # If user forget to call fixture(), BQUnit will raise exception,
    # and show table name which isn't mocked correctly.
    with pytest.raises(LookupError) as error_info:
        bqunit.test_query(tested_query)
        assert 'dummy-project-123456.dummy_dataset.dummy_table' in error_info.value


# todo:
# def test_parameterized_query():
# def test_table_with_suffix():

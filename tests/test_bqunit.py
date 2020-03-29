from bqunit.bqunit import BQUnit


def test_code_concept():
    # User have to prepare BQ project, and dataset for test bed
    bqunit = BQUnit(project_id='testing-project-999999', dataset_name='bqunit')
    bqunit.fixture(
        mocked_table_name='`dummy-project-123456.dummy_dataset.dummy_table`',
        statement="""
        select 1 as col1, 'str_1' as col2, true as col3
        union all
        select 2, 'str_2', false
        """)

    tested_query = """
       select col1, col2
       from `dummy_project_21341.dummy_dataset.dummy_table` foo
       where col1 = 1
     """
    job = bqunit.test_query(tested_query)
    # if pandas imported, user can do assertion by DataFrame.
    # df = job.to_dataframe()
    bqunit.teardown()
    assert job.result() == 'make_some_assertion'

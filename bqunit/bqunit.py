from google.cloud.bigquery import Client

from .query_builder import QueryBuilder
from .table_mapper import TableMapper


class BQUnit():
    __client: Client
    __mapper: TableMapper
    __builder: QueryBuilder

    def __init__(self, project_id: str, dataset_name: str):
        self.__client = Client(project=project_id)
        self.__mapper = TableMapper()
        self.__builder = QueryBuilder(dataset_name=dataset_name)

    def fixture(self, mocked_table_name: str, statement: str):
        self.__mapper.register(mocked_table_name)
        destination = self.__mapper.get(mocked_table_name)
        create_statement = self.__builder \
            .build_create_statement(destination, statement)
        query_job = self.__client.query(create_statement)
        query_job.result()  # Wait for the job to complete

    def test_query(self, query: str, job_config=None):
        # tables = self.__builder.extract_table_names(query)
        # todo: assert all tables are correctly fixtured
        query_to_test = self.__builder \
            .replace_by_dict(query, self.__mapper.as_dict())
        return self.__client.query(query_to_test, job_config=job_config)

    def teardown(self) -> None:
        targets = self.__mapper.redirected_tables()
        for target in targets:
            drop_statement = self.__builder.build_drop_statement(target)
            query_job = self.__client.query(drop_statement)
            query_job.result()  # Wait for the job to complete

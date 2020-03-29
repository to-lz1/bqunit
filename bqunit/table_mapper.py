import hashlib


class TableMapper:
    __dict = {}

    def register(self, source_table_name: str) -> None:
        self.__dict[source_table_name] = self.__get_hash(source_table_name)

    def registered_tables(self) -> list:
        return self.__dict.keys()

    def redirected_tables(self) -> list:
        return self.__dict.values()

    def get(self, key_table_name: str) -> str:
        return self.__dict[key_table_name]

    def as_dict(self) -> dict:
        return self.__dict

    def __get_hash(self, source_table_name: str) -> str:
        hash = hashlib.sha256(source_table_name.encode("utf-8")).hexdigest()
        return f't_{hash}'

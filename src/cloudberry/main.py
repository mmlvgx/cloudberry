import sqlite3

from typing import NewType


W = " "

Key = NewType("Key", str)
Value = NewType("Value", str)


class SQLTableParameter:
    def __init__(self, _name: str, _type: str) -> None:
        self._name = _name
        self._type = _type

        self.n = self._name
        self.t = self._type


class CloudberryConnection(sqlite3.Connection):
    def __init__(self, path: str, *args, **kwargs) -> None:
        super().__init__(path, *args, **kwargs)

        self._cursor = self.cursor()

    def createTable(self, name: str, *parameters: SQLTableParameter, override: bool = False) -> None:
        __sql = "CREATE TABLE"

        if override is False:
            __sql += f"{W}IF NOT EXISTS"

        __sql_table_parameters = f",{W}".join([f"{p.n}{W}{p.t}" for p in parameters])
        __sql += f"{W}{name}({__sql_table_parameters})"

        print(__sql)

        self._cursor.execute(__sql)
        super().commit()

    def insertIntoTable(self, name: str, key: Key | str, value: Value | str) -> None:
        __sql = f"INSERT INTO {name}({key})"
        __sql_insert_values = f"{W}VALUES('{value}')"

        __sql += __sql_insert_values

        print(__sql)

        self._cursor.execute(__sql)
        super().commit()

    def insertManyIntoTable(self, name: str, *keysAndValues: tuple[Key, Value]) -> None:
        __sql = f"INSERT INTO {name}"

        __sql_insert_keys = f",{W}".join([knv[0] for knv in keysAndValues])
        __sql_insert_values = f",{W}".join([f"'{knv[1]}'" for knv in keysAndValues])

        __sql = f'{__sql}({__sql_insert_keys}) VALUES({__sql_insert_values})'

        print(__sql)

        self._cursor.execute(__sql)
        super().commit()

    def selectFromTable(self, name: str, *keys: Key, where: tuple[Key, Value], all: bool=False) -> None:
        __sql = "SELECT"
        __sql_select_keys = "*"

        if not all:
            __sql_select_keys = f",{W}".join([k for k in keys])

        __sql = f"{__sql}{W}{__sql_select_keys} FROM {name}"

        if where:
            __sql_select_where_key_value = f"{where[0]} = '{where[1]}'"
            __sql += f"{W}WHERE {__sql_select_where_key_value}"

        print(__sql)

        self._cursor.execute(__sql)
        return super().cursor().fetchall()


class Cloudberry:
    def __init__(self, path: str) -> None:
        self.__path = path

    @property
    def path(self) -> str:
        return self.__path

    @path.setter
    def path(self, new: str) -> str:
        self.__path = new

    def connect(self) -> CloudberryConnection:
        return CloudberryConnection(self.path)


cloudberry = Cloudberry("users.db")
connection = cloudberry.connect()


# connection.createTable(
#     "users",
#     SQLTableParameter("id", "TEXT"),
#     SQLTableParameter("cash", "TEXT")
# )

# connection.insertManyIntoTable("users", ("id", "1"), ("cash", "0"))

# print(connection.selectFromTable("users", "cash", where=("id", 1)))
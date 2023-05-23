import mysql.connector
import mysql.connector.conversion
import aiomysql
import numpy as np
from sqlalchemy import create_engine


class MySQLDataManager:

    def __init__(self, database_host, database_user, database_password, database_name):
        self.database_host = database_host
        self.database_user = database_user
        self.database_password = database_password
        self.database_name = database_name
        self.connection = None
        self.pool = None

    async def connect_to_database(self):
        self.pool = await aiomysql.create_pool(
            host=self.database_host,
            user=self.database_user,
            password=self.database_password,
            db=self.database_name,
        )
        print("Connecté à la base de données.")

    async def close_connection(self):
        self.pool.close()
        await self.pool.wait_closed()
        print("Déconnecté de la base de données")

    async def get_all_tables(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW TABLES")
                tables = []
                async for row in cursor:
                    tables.append(row[0])
        return tables

    def add_dataframe_to_database(self, dataframe, table_name):
        cols = list(dataframe.columns)
        types = []

        for col in dataframe.dtypes:
            if col == 'object':
                types.append('VARCHAR(255)')
            elif col == 'datetime64[ns]' or col == 'datetime64[ns, UTC]':
                types.append('DATETIME')
            elif col == 'float64':
                types.append('FLOAT(2)')
            elif col == 'int64':
                types.append('INT(19)')
            elif col == 'bool':
                types.append('BOOLEAN')
            else:
                types.append('VARCHAR(255)')

        col_types = ', '.join([f'{col} {type}' for col, type in zip(cols, types)])

        create_table_query = f'CREATE TABLE `{table_name}` ({col_types})'

        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)

        self.connection.commit()

        try:
            query = f"INSERT INTO `{table_name}` ({', '.join(dataframe.columns)}) VALUES ({', '.join(['%s'] * len(dataframe.columns))})"

            data = [tuple(x) for x in dataframe.values]

            with self.connection.cursor() as cursor:
                print(cursor.executemany(query, data))

            self.connection.commit()

            print(f"Table {table_name} créée et données insérées avec succès!")

        except:
            engine = create_engine(f"mysql+mysqlconnector://{self.database_user}:{self.database_password}@{self.database_host}/{self.database_name}")

            dataframe.to_sql(name=table_name, con=engine, if_exists='append', index=False)

            self.close_connection()

    async def insert_or_update_new_data_with_specific_column(self, dataframe, table_name, column_to_check):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                if not dataframe.empty:
                    for index, row in dataframe.iterrows():
                        query_select = f"SELECT * FROM `{table_name}` WHERE {column_to_check} = {row[column_to_check]}"

                        try:
                            await cursor.execute(query_select)

                            result = await cursor.fetchone()

                            if result:
                                query_update = f"UPDATE `{table_name}` SET "

                                updates = []
                                for column, value in row.items():
                                    update = f"{column} = '{value}'"
                                    updates.append(update)

                                query_update += ", ".join(updates)
                                query_update += f" WHERE {column_to_check} = {row[column_to_check]}"

                                await cursor.execute(query_update)

                            else:
                                query_insert = f"INSERT INTO `{table_name}` ("
                                query_insert += ", ".join(row.keys())
                                query_insert += ") VALUES ("
                                query_insert += ", ".join([f"'{value.tolist()}'" if isinstance(value, np.ndarray) else f"'{value}'" for value in row.values])
                                query_insert += ")"

                                await cursor.execute(query_insert)

                        except mysql.connector.Error as e:
                            print(f"Erreur {e}")

                await conn.commit()


    async def insert_or_update_data_async(self, dataframe, table_name):
        if "id" in dataframe and not dataframe.empty:
            await self.insert_or_update_new_data_with_specific_column(dataframe, table_name, "id")
        elif "match_id" in dataframe and not dataframe.empty:
            await self.insert_or_update_new_data_with_specific_column(dataframe, table_name, "match_id")
        else:
            return


    async def get_table_id_list(self, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"SELECT id FROM `{table_name}`"
                await cursor.execute(query)
                id_list = []
                async for row in cursor:
                    id_list.append(row[0])
        return id_list
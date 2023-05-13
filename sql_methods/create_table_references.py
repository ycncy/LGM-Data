from sqlalchemy import create_engine, text


class Referencer:

    def __init__(self, database_path, username, password, database):
        self.database_path = database_path
        self.username = username
        self.password = password
        self.database = database
        self.connection = None
        self.engine = None

    def connect_to_database(self):
        self.engine = create_engine(f"mysql+mysqlconnector://{self.username}:{self.password}@{self.database_path}/{self.database}")

        self.connection = self.engine.connect()

        print("Connecté à la base de données.")

    def close_connection(self):
        self.engine.dispose()

        self.connection.close()

        print("Déconnecté de la base de données")

    def add_primary_and_foreign_keys(self, table_keys_array):
        for table_keys in table_keys_array:
            table_name = table_keys["table_name"]
            table_primary_keys = table_keys["table_primary_keys"]
            table_foreign_keys = table_keys["table_foreign_keys"]

            if len(table_primary_keys) > 0:
                for primary_key in table_primary_keys:
                    add_primary_keys_request = text(f"ALTER TABLE" + f"`{table_name}`" + f"ADD CONSTRAINT {table_name}_primary_key_{primary_key} PRIMARY KEY ({primary_key})")

                    self.connection.execute(add_primary_keys_request)

                print("Clé(s) primaire(s) ajoutée(s)")

            if len(table_foreign_keys) > 0:
                for table_foreign_key_info in table_foreign_keys:

                    column_name = table_foreign_key_info["column_name"]
                    reference_table_name = table_foreign_key_info["reference_table_name"]
                    reference_column_name = table_foreign_key_info["reference_column_name"]

                    add_foreign_keys_request = text(f"ALTER TABLE {table_name} ADD CONSTRAINT foreign_key_constraint_{table_name}_{column_name} FOREIGN KEY ({column_name}) REFERENCES {reference_table_name}({reference_column_name})")

                    self.connection.execute(add_foreign_keys_request)

                print("Référence(s) et clé(s) étrangère(s) ajoutées")
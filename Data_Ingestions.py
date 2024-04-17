import polars as pl
from sqlalchemy import create_engine, engine
import os
import argparse
from urllib.request import urlretrieve

class Connection_Database(object):

    def __init__(self, user: str, password: str, database: str, host: str, port: int):

        self.user      = user
        self.password  = password
        self.database  = database
        self.host      = host
        self.port      = port

    def conn_engine(self) -> engine:
        
        return create_engine(self.get_engine)

    def get_engine(self) -> str:
        
        engine_db = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

        return engine_db

class Ingestion(object):

    def __init__(self, url: str, file: str, table_name: str):
    
        self.url         = url
        self.file        = file
        self.table_name  = table_name

    def get_data(self):

        if os.path.exists(self.file) is False:
            urlretrieve(self.url, self.file)

    def read_data(self) -> pl.DataFrame:
        
        self.get_data()

        if self.file.endswith(".parquet") or self.file.endswith(".pq"):
            
            df = pl.read_parquet(self.file, low_memory=True)
        
        elif self.file.endswith(".csv"):
            
            df = pl.read_csv(self.file, low_memory=False)
        
        elif self.file.endswith(".json"):

            df = pl.read_json(self.file, low_memory=False)
        
        elif self.file.endswith(".xlsx") or self.file.endswith(".xls") or self.file.endswith(".xlt") or self.file.endswith(".xml"):

            df = pl.read_excel(self.file, low_memory=False)
        
        else:

            print("type data not found or unknown")
        
        return df
            
    def send_database(self, df: pl.DataFrame, db: str):
        
        df.write_database(self.table_name, db, if_table_exists="replace")


if __name__ == "__main__":

    try:
        
        parser = argparse.ArgumentParser()

        parser.add_argument("-U", "--user", help="Postgres user")
        parser.add_argument("-p", "--password", help="Postgres password")
        parser.add_argument("-db", "--database", help="Postgres database")
        parser.add_argument("-H", "--host", help="Postgres host")
        parser.add_argument("-P", "--port", help="Postgres port", default=5432)
        parser.add_argument("-f", "--file", help="Filepath to read")
        parser.add_argument("-t", "--table_name", help="Table name to write/update")
        parser.add_argument("-u", "--url", help="URL data source")
        
        args = parser.parse_args()

        connection  = Connection_Database(args.user, args.password, args.database, args.host, args.port)
        ingestion   = Ingestion(args.url, args.file, args.table_name)
        db          = connection.get_engine()

        dataframe = ingestion.read_data()
        ingestion.send_database(dataframe, db)
            
        print("upload data to database success")
    
    except:

        print("unsuccess upload data to database")
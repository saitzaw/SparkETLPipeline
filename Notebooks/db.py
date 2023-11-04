from os import getenv
from os.path import join
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine

dotenv_path = join(Path(__file__).resolve().parents[0], '.env')
load_dotenv(dotenv_path)

class DBConnx:
    def __init__(self):
        dw_db = str(getenv("SRC_DB"))
        dw_conn = str(getenv("SRC_CONN"))
        ext_db = str(getenv("EXT_DB"))
        ext_conn = str(getenv("EXT_CONN"))
        int_db = str(getenv("INT_DB"))
        int_conn = str(getenv("INT_CONN"))
        sg_db = str(getenv("STAGING_DB"))
        sg_conn = str(getenv("STAGING_CONN"))
        self.dw_driver = f"{dw_db}+{dw_conn}"
        self.sg_driver = f"{sg_db}+{sg_conn}"
        self.int_driver = f"{int_db}+{int_conn}"
        self.ext_driver = f"{ext_db}+{ext_conn}"
        

    def source_dw_engine(self):
        url = URL.create(
            self.dw_driver,
            username = str(getenv("SRC_UNAME")),
            password = str(getenv("SRC_PASS")),
            host = str(getenv("SRC_HOST")),
            database = str(getenv("SRC_DB_NAME"))
        )
        return create_engine(url)
    
    
    def csm_staging_engine(self):
        url = URL.create(
            self.sg_driver,
            username = str(getenv("STAGING_UNAME")),
            password = str(getenv("STAGING_PASS")),
            host = str(getenv("STAGING_HOST")),
            database = str(getenv("STAGING_DB_NAME"))
        )
        return create_engine(url)
    
    
    def csm_int_engine(self):
        url = URL.create(
            self.int_driver,
            username = str(getenv("INT_UNAME")),
            password = str(getenv("INT_PASS")),
            host = str(getenv("INT_HOST")),
            database = str(getenv("INT_DB_NAME"))
        )
        return create_engine(url)
    
        
    def csm_ext_engine(self):
        url = URL.create(
            self.ext_driver,
            username = str(getenv("EXT_UNAME")),
            password = str(getenv("EXT_PASS")),
            host = str(getenv("EXT_HOST")),
            database = str(getenv("EXT_DB_NAME"))
        )
        return create_engine(url)
    
class SinkData:
    def __init__(self, choose_db: str = 'staging'):
        if choose_db == 'staging':
            self.engine = DBConnx().csm_staging_engine()
        elif choose_db == 'ext':
            self.engine = DBConnx().csm_ext_engine()
        elif choose_db == "int":
            self.engine = DBConnx().csm_int_engine()
        elif choose_db == 'fdw':
            self.engine = DBConnx().source_dw_engine()
    
    def bulk_insert(self, df, table_class):
        from sqlalchemy.exc import SQLAlchemyError
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=self.engine)
        session = Session()
        status = False
        try:
            data = [table_class(**row) for i, row in df.iterrows()]
            session.bulk_save_objects(data)
            session.commit()
            status = True
        except SQLAlchemyError as e:
            session.rollback()
            status = False
            print(str(e.orig))
        finally:
            self.engine.dispose()
        return status

    def bulk_insert2(self,df, table):
        conn = self.connect()
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL query to execute
        print(table)
        query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, sql.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            conn.close()
            raise error
        print("execute_values() done")
        cursor.close()
        conn.close()

    def insert_many(self, df , table_name: str,) -> bool:
        chunks = self.chunk
        engine = self.connect()
        try:
            df_split = np.array_split(df, chunks)
            for i, sdf in enumerate(df_split, chunks):
                sdf.to_sql(
                    table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=10000
                )
        except (Exception, sql.exc.SQLAlchemyError) as error:
            raise error
        finally:
            engine.dispose()
        return True
    
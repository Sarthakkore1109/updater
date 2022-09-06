import os
import mysql.connector
from dotenv import load_dotenv
import pandas as pd

if __name__ == '__main__':
    print('hello world')

    load_dotenv()

    mysql_host = os.getenv('mysql_host')
    mysql_user = os.getenv('mysql_user')
    mysql_pw = os.getenv('mysql_pw')

    pharmacogenomics_db = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_pw,
        db="pharmacogenomics_dev")

    cursor = pharmacogenomics_db.cursor(buffered=True, dictionary=True)

    df = pd.read_csv('merged_filtered.csv')
    print(df.iloc[0,3])

    '''sql = f"""
    SELECT distinct drugID, SmileCode
    FROM `precursors`"""'''

    for i in range(len(df)):
        sql = f""" UPDATE gtexome_gtex
            SET developmental_level = \"{df.iloc[i,3]}\"
            WHERE gene_id = \"{df.iloc[i,1]}\"
            """

        try:
            cursor.execute(sql)
            pharmacogenomics_db.commit()
            print(str(i)+" done")

            if i % 100 == 0:
                pharmacogenomics_db.commit()
                print(str(i)+" committed")

        except:
            print('exception')
            pharmacogenomics_db.rollback()

    pharmacogenomics_db.commit()
    cursor.close()

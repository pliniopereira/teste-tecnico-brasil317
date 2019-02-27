import json
import os

import pandas as pd
from pymongo import MongoClient


def import_content(filepath):
    """
    Funcao que converte csv em arquivo json • Armazene os dados desta página em um documento do MongoDB na nuvem
    Atlas
    """
    path_to_save = filepath.split('/')[1]

    try:
        client = MongoClient("mongodb://admin:adminadmin@cluster0-shard-00-00-cjouw.mongodb.net:27017,cluster0-shard-00-01-cjouw.mongodb.net:27017,cluster0-shard-00-02-cjouw.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true")
        db = client[str(path_to_save)]
        m_collection = db.files
        cdir = os.path.dirname(__file__)
        file_res = os.path.join(cdir, filepath)

        data = pd.read_csv(file_res)
        data_json = json.loads(data.to_json(orient='records'))
        print(data_json)
        # m_collection.remove()
        m_collection.insert_many(data_json)

        # m_collection.insert(data_json)
    except Exception as e:
        print("Failed Mongo!\n{}".format(e))
    print("Mongo")


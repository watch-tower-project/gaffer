import traceback

import pymongo
from common_constants import node_queue_map_collection
from . import logger
from pymongo import errors


class GafferConfig:
    def __init__(self, cloud_key: str, cloud_secret: str, cloud_region: None, mongo_uri: str, db_name: str):
        self.cloud_key = cloud_key
        self.cloud_secret = cloud_secret
        self.cloud_region = cloud_region
        self.mongo_uri = mongo_uri
        self.db_name = db_name

        def __create_db_client(self):
            mongo_connection = pymongo.MongoClient(self.mongo_uri)
            mongo_db = mongo_connection[self.db_name]
            return mongo_db

        self.db_client = __create_db_client()

    def __node_queue_map_db(self):
        return self.db_client[node_queue_map_collection]

    def add_node_queue_map(self, queue_address, node_name):
        try:
            self.__node_queue_map_db().insert({
                "queue": queue_address,
                "node": node_name
            })
        except errors.ConnectionFailure:
            traceback.print_exc()
            logger.critical("FAILED TO CONNECT TO DB CLIENT")
        except errors.DuplicateKeyError:
            traceback.print_exc()
            logger.error("NODE OR QUEUE ALREADY EXIST IN THE MESH")

    def remove_node_queue_map(self, node_name):
        try:
            self.__node_queue_map_db().remove({
                "node": node_name
            })
        except errors.ConnectionFailure:
            traceback.print_exc()
            logger.critical("FAILED TO CONNECT TO DB CLIENT")

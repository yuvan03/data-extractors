from pymongo import MongoClient


class MongoExtractor():

    def __init__(self, db_url, db_name, collection_name):
        self.client = MongoClient(db_url)
        self.urlCollection = None
        self.url_cursor = None
        self.set_url_connection(db_name, collection_name)
        self.set_url_cursor()

    def set_url_connection(self, db_name, collection_name):
        """
        Sets connection for [collection_name] in [db_name]

        :param db_name: db name
        :param collection_name: collection name
        :return:
        """
        self.urlCollection = self.client[db_name][collection_name]

    def set_url_cursor(self):
        """
        Sets cursor for connection
        :return:
        """
        self.url_cursor = self.urlCollection.aggregate([{'$unwind': {'path': "$client_info.website_info", 'preserveNullAndEmptyArrays': True}}])

    def insert_record(self, article, db_url):
        """
        Inserts record with [article] contents into MongoDB

        :param article: contents of record
        :param db_url: db url
        :return:
        """
        client = self.client
        if db_url is not None:
            client = MongoClient(db_url)

        db = client.output_database
        db.output_collection.insert_one({'doc': article})


if __name__ == '__main__':
    db_url = 'mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017/?authSource=admin'
    db_name = 'sample_db'
    collection_name = 'sample_collection'
    MongoExtractor(db_url, db_name, collection_name)

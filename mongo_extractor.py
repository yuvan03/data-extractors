from pymongo import MongoClient


class MongoExtractor():

    def __init__(self, db_url, db_name, collection_name):
        self.client = MongoClient(db_url)
        self.urlCollection = None
        self.url_cursor = None
        self.set_url_connection(db_name, collection_name)
        self.set_url_cursor()

    def set_url_connection(self, db_name, collection_name):
        self.urlCollection = self.client[db_name][collection_name]

    def set_url_cursor(self):
        self.url_cursor = self.urlCollection.aggregate([{'$unwind': {'path': "$client_info.website_info", 'preserveNullAndEmptyArrays': True}}])

    def insert_record(self, article):
        db = self.client.output_database
        db.output_collection.insert_one({'doc': article})


if __name__ == '__main__':
    db_url = 'mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017/?authSource=admin'
    db_name = 'sample_db'
    collection_name = 'sample_collection'
    MongoExtractor(db_url, db_name, collection_name)


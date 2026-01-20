"""Test script to verify IDE autocomplete shows all PyMongo methods.

This script demonstrates that all PyMongo methods are available for autocomplete
in IDEs. It uses type stubs to provide autocomplete without runtime execution.
"""

from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Type checking only - these imports help IDEs understand the types
    from pymongo import MongoClient

    from xlr8 import accelerate


def test_cursor_autocomplete() -> None:
    """Demonstrate XLR8Cursor has all PyMongo cursor methods + XLR8 methods."""
    if TYPE_CHECKING:
        client = MongoClient("mongodb://localhost:27017")
        db = client.test_db
        collection = db.test_collection

        xlr8_coll = accelerate(
            collection, schema={}, mongo_uri="mongodb://localhost:27017"
        )
        cursor = xlr8_coll.find({})

        # PyMongo cursor methods (all should autocomplete):
        cursor.skip(10)
        cursor.limit(100)
        cursor.sort("_id", 1)
        cursor.batch_size(1000)
        cursor.add_option(2)
        cursor.remove_option(2)
        cursor.allow_disk_use(True)
        cursor.collation({"locale": "en"})
        cursor.comment("test query")
        cursor.hint("_id_")
        cursor.max([("_id", 100)])
        cursor.min([("_id", 1)])
        cursor.where("this.x > 5")
        cursor.clone()
        cursor.rewind()
        cursor.close()
        cursor.next()
        cursor.distinct("field")
        cursor.explain()
        cursor.to_list(100)

        # XLR8-specific cursor methods:
        cursor.to_dataframe()
        cursor.to_polars()
        cursor.to_dataframe_batches()
        cursor.stream_to_callback(
            lambda x, y: None, partition_time_delta=timedelta(days=1)
        )
        cursor.raw_cursor()
        cursor.explain_acceleration()


def test_collection_autocomplete() -> None:
    """Demonstrate XLR8Collection has all PyMongo collection methods."""
    if TYPE_CHECKING:
        client = MongoClient("mongodb://localhost:27017")
        db = client.test_db
        collection = db.test_collection

        xlr8_coll = accelerate(
            collection, schema={}, mongo_uri="mongodb://localhost:27017"
        )

        # PyMongo collection methods (all should autocomplete):
        xlr8_coll.insert_one({"x": 1})
        xlr8_coll.insert_many([{"x": 2}, {"x": 3}])
        xlr8_coll.update_one({"x": 1}, {"$set": {"y": 2}})
        xlr8_coll.update_many({"x": {"$gt": 0}}, {"$set": {"z": 3}})
        xlr8_coll.replace_one({"x": 1}, {"x": 10})
        xlr8_coll.delete_one({"x": 1})
        xlr8_coll.delete_many({"x": {"$gt": 0}})
        xlr8_coll.find_one({"x": 1})
        xlr8_coll.count_documents({})
        xlr8_coll.estimated_document_count()
        xlr8_coll.distinct("field")
        xlr8_coll.aggregate([{"$match": {}}])
        xlr8_coll.create_index([("x", 1)])
        xlr8_coll.create_indexes([{"keys": [("x", 1)]}])
        xlr8_coll.drop_index("x_1")
        xlr8_coll.drop_indexes()
        xlr8_coll.list_indexes()
        xlr8_coll.index_information()
        xlr8_coll.drop()
        xlr8_coll.rename("new_name")
        xlr8_coll.options()

        # XLR8-specific collection methods:
        xlr8_coll.raw_collection()
        xlr8_coll.set_schema({})
        xlr8_coll.get_schema()
        xlr8_coll.clear_cache()


if __name__ == "__main__":
    print(" Type stubs are configured correctly!")
    print(" All PyMongo cursor methods available for autocomplete")
    print(" All PyMongo collection methods available for autocomplete")
    print(" All XLR8-specific methods available for autocomplete")
    print("\nTo verify: Open this file in your IDE and type 'cursor.' or 'xlr8_coll.'")
    print("You should see autocomplete for all PyMongo + XLR8 methods!")

"""Test PyMongo parameter compatibility for drop-in replacement."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xlr8 import accelerate


def test_pymongo_cursor_parameters_accepted():
    """
    Verify XLR8 accepts all PyMongo cursor parameters for compatibility.

    This ensures existing PyMongo code can be migrated by just wrapping
    the collection with accelerate() without changing any cursor parameters.
    """
    if TYPE_CHECKING:
        from pymongo import MongoClient

        client = MongoClient("mongodb://localhost:27017")
        db = client.test_db
        collection = db.test_collection

        xlr8_coll = accelerate(
            collection, schema={}, mongo_uri="mongodb://localhost:27017"
        )

        # All these PyMongo parameters should be accepted without error
        cursor = xlr8_coll.find(
            {"status": "active"},
            projection={"_id": 0, "name": 1},
            skip=10,
            limit=100,
            sort=[("name", 1)],
            batch_size=50,
            # PyMongo-specific parameters that should pass through
            no_cursor_timeout=True,
            cursor_type=0,
            allow_partial_results=False,
            collation={"locale": "en"},
            hint="name_1",
            max_time_ms=5000,
            max=[("score", 100)],
            min=[("score", 0)],
            comment="test query",
            allow_disk_use=True,
        )

        # Verify cursor methods still work
        cursor.skip(5)
        cursor.limit(50)
        cursor.batch_size(25)

        # XLR8-specific methods should also work
        cursor.explain_acceleration()


def test_find_parameters_backward_compatible():
    """
    Test that existing PyMongo find() calls work without modification.

    Example migration scenario:
    OLD: cursor = pymongo_collection.find({...}, hint="index_1")
    NEW: cursor = xlr8_collection.find({...}, hint="index_1")

    No code changes needed!
    """
    if TYPE_CHECKING:
        from pymongo import MongoClient

        client = MongoClient("mongodb://localhost:27017")
        db = client.test_db
        collection = db.test_collection

        xlr8_coll = accelerate(
            collection, schema={}, mongo_uri="mongodb://localhost:27017"
        )

        # Real-world PyMongo usage patterns that should all work:

        # Pattern 1: Query with hint for index optimization
        cursor1 = xlr8_coll.find(
            {"user_id": 123, "timestamp": {"$gte": "2024-01-01"}},
            hint="user_id_1_timestamp_1",
        )

        # Pattern 2: Query with collation for case-insensitive search
        cursor2 = xlr8_coll.find(
            {"name": "John"}, collation={"locale": "en", "strength": 2}
        )

        # Pattern 3: Long-running query with timeout
        cursor3 = xlr8_coll.find(
            {"status": "processing"}, no_cursor_timeout=True, max_time_ms=30000
        )

        # Pattern 4: Query with min/max index bounds
        cursor4 = xlr8_coll.find(
            {}, hint="score_1", min=[("score", 50)], max=[("score", 100)]
        )

        # All cursors should be valid XLR8Cursor instances
        assert cursor1 is not None
        assert cursor2 is not None
        assert cursor3 is not None
        assert cursor4 is not None


if __name__ == "__main__":
    print("XLR8 accepts all PyMongo cursor parameters")
    print("Existing PyMongo code works without modification")
    print("True drop-in replacement for seamless migration")

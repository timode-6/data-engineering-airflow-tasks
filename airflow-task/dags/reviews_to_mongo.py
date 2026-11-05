from __future__ import annotations

import pendulum

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk import DAG, task

from config import (
    FINAL_FILE,
    MONGO_COLLECTION,
    MONGO_CONN_ID,
    MONGO_DATABASE,
    REVIEWS_PROCESSED,
)

BATCH_SIZE = 5_000
CHUNK_SIZE = 50_000


with DAG(
    dag_id="reviews_to_mongo",
    description="Load processed reviews into MongoDB when the asset updates",
    schedule=[REVIEWS_PROCESSED],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["reviews", "mongodb"],
) as dag:

    @task
    def load_to_mongodb() -> dict[str, int]:
        import pandas as pd

        inserted = 0
        with MongoHook(mongo_conn_id=MONGO_CONN_ID) as hook:
            collection = hook.get_collection(MONGO_COLLECTION, MONGO_DATABASE)
            collection.delete_many({})

            collection.create_index("created_date")

            reader = pd.read_csv(
                FINAL_FILE,
                dtype=str,
                keep_default_na=False,
                na_values=[],
                chunksize=CHUNK_SIZE,
            )

            for chunk in reader:
                chunk = chunk.rename(columns={"reviewId": "_id"})
                chunk["created_date"] = pd.to_datetime(
                    chunk["created_date"], format="mixed"
                )
                for column in ("score", "thumbsUpCount"):
                    chunk[column] = pd.to_numeric(chunk[column], errors="coerce")

                documents = chunk.to_dict("records")
                for start in range(0, len(documents), BATCH_SIZE):
                    batch = documents[start : start + BATCH_SIZE]
                    collection.insert_many(batch, ordered=False)
                    inserted += len(batch)

        print(f"inserted {inserted} documents into {MONGO_DATABASE}.{MONGO_COLLECTION}")
        return {"inserted": inserted}

    @task
    def verify_load(load_stats: dict[str, int]) -> dict[str, int]:
        import pandas as pd

        expected = sum(
            len(chunk)
            for chunk in pd.read_csv(
                FINAL_FILE, dtype=str, keep_default_na=False,
                na_values=[], chunksize=CHUNK_SIZE,
            )
        )

        with MongoHook(mongo_conn_id=MONGO_CONN_ID) as hook:
            collection = hook.get_collection(MONGO_COLLECTION, MONGO_DATABASE)
            actual = collection.count_documents({})

            if actual != expected:
                raise ValueError(
                    f"loaded {actual} documents but the file has {expected} rows"
                )

            top_five = list(
                collection.aggregate([
                    {"$group": {"_id": "$content", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1, "_id": 1}},
                    {"$limit": 5},
                ])
            )
            short = collection.count_documents(
                {"$expr": {"$lt": [{"$strLenCP": "$content"}, 5]}}
            )
            days = list(
                collection.aggregate([
                    {"$group": {
                        "_id": {"$dateTrunc": {"date": "$created_date", "unit": "day"}},
                        "average_rating": {"$avg": "$score"},
                    }},
                    {"$sort": {"_id": 1}},
                ])
            )

        print(f"documents:              {actual}")
        print(f"content shorter than 5: {short}")
        print(f"distinct days:          {len(days)}")
        print("top five comments:")
        for row in top_five:
            print(f"    {row['count']:>6}  {row['_id']!r}")
        if days:
            print(f"first day: {days[0]['_id']} avg={days[0]['average_rating']:.4f}")
            print(f"last day:  {days[-1]['_id']} avg={days[-1]['average_rating']:.4f}")

        return {"documents": actual, "short_content": short, "days": len(days)}

    verify_load(load_to_mongodb())

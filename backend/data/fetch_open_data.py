from sqlalchemy.orm import Session
from database.schemas import Counter, Count
from database.utils_db import engine, init_db
import pandas as pd

def insert_data(df: pd.DataFrame):
    session = Session(bind=engine)

    try:
        # 1 — Upsert des compteurs
        for _, row in df[["counter_id", "counter_name"]].drop_duplicates().iterrows():
            counter = session.query(Counter).filter_by(counter_id=row["counter_id"]).first()

            if not counter:
                counter = Counter(
                    counter_id=row["counter_id"],
                    name=row["counter_name"]
                )
                session.add(counter)
            else:
                counter.name = row["counter_name"]

        session.commit()

        # 2 — Insertion ou mise à jour des mesures
        for _, row in df.iterrows():
            existing = session.query(Count).filter_by(
                counter_id=row["counter_id"],
                ts=row["ts"]
            ).first()

            if existing:
                existing.count = row["count"]
            else:
                new_count = Count(
                    counter_id=row["counter_id"],
                    ts=row["ts"],
                    count=row["count"]
                )
                session.add(new_count)

        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()

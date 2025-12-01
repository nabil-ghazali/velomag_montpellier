import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://velo:velo@db:5432/velo_db")

engine = create_engine(DATABASE_URL)

def init_db():
    """Crée les tables à partir de schema.sql."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        conn.commit()
    print("Base initialisée (schema.sql appliqué).")

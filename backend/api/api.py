from fastapi import FastAPI, HTTPException, Query
from contextlib import asynccontextmanager
from typing import List, Optional
from datetime import datetime
import os

from dotenv import load_dotenv
from sqlalchemy import select

from database.schemas import Database  # car tu es dans backend/ et tu fais "python -m api.main"

# Charger les variables d'environnement (.env)
load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

db = Database(DATABASE_URL)

app = FastAPI(title="Vélo Montpellier API", lifespan="1.0.0")

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/counters")
def get_counters():
    """
    Retourne la liste des compteurs (counters_clean).
    """
    with db.engine.connect() as conn:
        result = conn.execute(
            select(
                db.velo_clean.c.counter_id,
                db.velo_clean.c.lat,
                db.velo_clean.c.lon,
                db.velo_clean.c.datetime,
            )
        )
        counters = [
            {
                "counter_id": row.counter_id,
                "lat": row.lat,
                "lon": row.lon,
                "intensity_example": row.intensity,
            }
            for row in result
        ]

    return {"counters": counters}


# --- Route pour récupérer le trafic d'un compteur sur une période ---
@app.get("/predict")
def get_predict(
    

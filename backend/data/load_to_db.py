from database.schemas import Database
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

db = schemas.Database(SUPABASE_URL, SUPABASE_SERVICE_KEY)
db.create_tables()

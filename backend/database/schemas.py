from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, Float, Boolean, NullPool, create_engine, text

class Database:

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, database_url: str):
        if getattr(self, "_initialized", False):
            return

        self._initialized = True

        self.metadata = MetaData()
        self.engine = create_engine(database_url, poolclass=NullPool)

        self.counters_raw = None
        self.counters_clean = None
        self.meteo_raw = None
        self.meteo_clean = None
        self.model_data = None

    def create_tables(self):

        self.counters_clean = Table(
            "counters_clean",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("counter_id", String, unique=True, nullable=False),
            Column("intensity", Float, nullable=False),
            Column("lat", Float, nullable=True),
            Column("lon", Float, nullable=True),
            Column("weekday", Integer, nullable=True),
            Column("is_weekend", Boolean, nullable=True),
            Column("hour", Integer, nullable=True),
        )

        self.counters_raw = Table(
            "counters_raw",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("counter_id", String, nullable=False),
            Column("intensity", Float, nullable=False),
            Column("lat", Float, nullable=True),
            Column("lon", Float, nullable=True),
            Column("laneId", Integer, nullable=True),
            Column("vehicleType", String, nullable=True),
        )

        self.meteo_raw = Table(
            "meteo_raw",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("temperature_2m", Float, nullable=False),
            Column("wind_speed_10m", Float, nullable=False),
            Column("precipitation", Float, nullable=True),
        )

        self.meteo_clean = Table(
            "meteo_clean",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("temperature_2m", Float, nullable=False),
            Column("wind_speed_10m", Float, nullable=False),
            Column("precipitation", Float, nullable=True),
        )

        self.model_data = Table(
            "model_data",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("datetime", DateTime, nullable=False),
            Column("predicted_values", Float, nullable=False),
        )

        self.metadata.create_all(self.engine)

        with self.engine.begin() as conn:
            for table in [
                self.counters_raw,
                self.counters_clean,
                self.meteo_raw,
                self.meteo_clean,
                self.model_data,
            ]:
                conn.execute(
                    text(
                        f'ALTER TABLE "public"."{table.name}" ENABLE ROW LEVEL SECURITY;'
                    )
                )

    def drop_tables(self, name: str = None):
        if name is None:
            self.metadata.drop_all(self.engine)
        else:
            table = Table(name, self.metadata, autoload_with=self.engine)
            table.drop(self.engine, checkfirst=True)

           



    


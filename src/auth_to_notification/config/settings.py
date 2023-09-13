from pydantic import BaseSettings


class Settings(BaseSettings):
    elastic_endpoint: str
    elastic_movie_index: str = "movies"
    elastic_person_index: str = "persons"
    elastic_genre_index: str = "genres"
    elastic_pack_size: int = 1000

    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: str

    redis_host: str
    redis_port: int

    tables_for_scan: list[tuple[str, int]] = [("user", 1000)]
    schema_from: str = "users"
    schema_to: str = "notifications"

    wait_up_to: int = 60 * 60 * 12
    waiting_interval: int = 60 * 30
    waiting_factor: int = 2
    first_nap: float = 0.1


settings = Settings()  # type: ignore

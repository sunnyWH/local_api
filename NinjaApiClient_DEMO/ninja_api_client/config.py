from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ninja_api_", env_file=(".env", ".env.prod"), env_file_encoding="utf-8")

    trading_host: str = Field(default="127.0.0.1")
    trading_port: int = Field(default=58000)
    trading_user: str
    trading_password: str
    trading_access_token: str
    positions_host: str = Field(default="127.0.0.1")
    positions_port: int = Field(default=58001)
    positions_access_token: Optional[str] = None

settings = Settings()

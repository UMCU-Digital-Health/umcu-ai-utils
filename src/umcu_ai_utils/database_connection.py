import logging
import os
from typing import Optional

from sqlalchemy import Engine, create_engine

logger = logging.getLogger(__name__)


def get_connection_string(
        use_debug_sqlite: bool = False,
        db_env: str | None = None,
        db_user: str | None = None,
        db_passwd: str | None = None,
        db_host: str | None = None,
        db_port: str | None = None,
        db_database: str | None = None,        
        schema_name: str | None = None,
) -> tuple[str, Optional[dict]]: 
    """Get the connection string from the environment variables    
    Parameters
    ----------
    use_debug_sqlite : bool, optional
        If True, use the SQLite debug database, by default False
        If True, the other parameters are ignored
    db_env : str, optional
        The Posit Connect database environment to use, by default "None", alternatively 'ACC' or 'PROD'
        If None, the default environment configured in the environment variables is used
    db_user : str, optional
        Username of the service account, by default None
    db_passwd : str, optional
        Password of the service account, by default None
    db_host : str, optional
        Host of the database, by default None
    db_port : str, optional
        Database port, by default None
    db_database : str, optional
        Database name, by default None
    schema_name : str, optional
        The schema name of the database, by default None.
        Only needs to be set to remove it when using the SQLite debug database.

    Returns
    -------
    str
        The connection string to the database
    """
    db_user = db_user or os.getenv("DB_USER", "")
    db_passwd = db_passwd or os.getenv("DB_PASSWD")
    db_port = db_port or os.getenv("DB_PORT")
    if db_env is None:
        db_host = db_host or os.getenv("DB_HOST")
        db_database = db_database or os.getenv("DB_DATABASE")
    elif db_env == "ACC" or db_env == "PROD":
        db_host = db_host or os.getenv(f"DB_HOST_{db_env}")
        db_database = db_database or os.getenv(f"DB_DATABASE_{db_env}")
    else:
        raise ValueError(f"Invalid environment: {db_env}")

    if db_user == "":
        logger.warning("Using debug SQLite database...")

        return "sqlite:///./sql_app.db", {"schema_translate_map": {schema_name: None}}

    logger.info(f"Connecting to {db_host} and database {db_database}")

    return (
        f"mssql+pymssql://{db_user}:{db_passwd}@{db_host}:{db_port}/{db_database}",
        None,
    )


def get_engine(
    connection_str: str | None = None,
    db_env: str | None = None,
    schema_name: str | None = None,
) -> Engine:
    """Get the SQLAlchemy engine

    Optionally use the parameter to override the connection string

    Parameters
    ----------
    connection_str : str, optional
        The connection string to the database, by default None
    db_env : str, optional
        The environment to use, by default None, alternatively 'ACC' or 'PROD'
        If None, the default environment configured in the environment variables is used
        Only used when connection_str is None
    schema_name : str, optional
        The schema name of the database, by default None.
        Only needs to be set to remove it when using the SQLite debug database.

    Returns
    -------
    create_engine
        The SQLAlchemy engine used for queries
    """
    if connection_str is None:
        connection_str, execution_options = get_connection_string(
            env=db_env, schema_name=schema_name
        )
    else:
        connection_str, execution_options = connection_str, None
    return create_engine(
        connection_str, pool_pre_ping=True, execution_options=execution_options
    )

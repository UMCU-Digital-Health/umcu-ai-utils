import pytest
from sqlalchemy.engine import Engine

from umcu_ai_utils import database_connection


def test_get_connection_string_sqlite():
    conn_str, exec_opts = database_connection.get_connection_string(
        db_env="DEBUG", schema_name="test_schema"
    )
    assert conn_str.render_as_string(hide_password=False) == "sqlite:///./sql_app.db"
    assert exec_opts == {"schema_translate_map": {"test_schema": None}}


def test_get_connection_string_missing_params(monkeypatch):
    for var in ["DB_USER", "DB_PASSWD", "DB_HOST", "DB_PORT", "DB_DATABASE"]:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ValueError) as exc:
        database_connection.get_connection_string()
    assert "Database connection parameters are not all set" in str(exc.value)


def test_get_connection_string_valid(monkeypatch):
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWD", "pass")
    monkeypatch.setenv("DB_HOST", "host")
    monkeypatch.setenv("DB_PORT", "1433")
    monkeypatch.setenv("DB_DATABASE", "db")
    conn_str, exec_opts = database_connection.get_connection_string()
    assert (
        conn_str.render_as_string(hide_password=False)
        == "mssql+pymssql://user:pass@host:1433/db"
    )
    assert exec_opts is None


def test_escape_special_characters_in_connection_string():
    """Test that special characters in the password are properly escaped"""
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWD", "p@ssw:rd")  # Special characters
    monkeypatch.setenv("DB_HOST", "host")
    monkeypatch.setenv("DB_PORT", "1433")
    monkeypatch.setenv("DB_DATABASE", "db")
    conn_str, exec_opts = database_connection.get_connection_string()
    assert (
        conn_str.render_as_string(hide_password=False)
        == "mssql+pymssql://user:p%40ssw%3Ard@host:1433/db"
    )
    assert exec_opts is None


def test_get_engine_sqlite():
    engine = database_connection.get_engine(db_env="DEBUG", schema_name="test_schema")
    assert isinstance(engine, Engine)
    assert str(engine.url).startswith("sqlite:///"), "Engine should use SQLite URL"


def test_get_engine_with_connection_str():
    conn_str = "sqlite:///./sql_app.db"
    engine = database_connection.get_engine(connection_str=conn_str)
    assert isinstance(engine, Engine)
    assert str(engine.url) == conn_str

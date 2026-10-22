from alembic import context

from app.core.db import Base, get_engine
from app.models import entities  # noqa: F401


def run():
    with get_engine().connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=Base.metadata,
            render_as_batch=connection.dialect.name == "sqlite",
        )
        with context.begin_transaction():
            context.run_migrations()


run()

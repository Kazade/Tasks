
import os

DATABASE_ENGINE = "sqlite3"
#FIXME: Use XDG directory
DATABASE_NAME = os.path.join(os.path.expanduser("~"), "tasks.sqlite3")

INSTALLED_APPS = [
    "tasks"
]


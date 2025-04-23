from app.repositories.baseRepositories import BaseRepositories

class UserRepository(BaseRepositories):
    def __init__(self):
        self._table = "users"
        parent(self._table)

    def _list():
        return parent()._list()
"""Error raised when the coordinator returns an error response."""


class CoordinatorError(Exception):
    def __init__(self, status_code, code, message):
        super().__init__(f"{status_code} {code}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message

class AppError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status
        super().__init__(message)


class ProcessingError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


class LeaseLost(Exception):
    pass


class Cancelled(Exception):
    pass

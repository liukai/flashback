class InvalidOperation(Exception):

    """Raised when user attempts to perform invalid operation."""

    def __init__(self, reason):
        Exception.__init__(self)
        self.reason = reason

    def __str__(self):
        return self.reason

"""The error every pipeline stage (fetch, map, push) raises."""


# ---------------------------------------------------------------- errors
class LeadError(Exception):
    def __init__(self, stage, cls, message, http_status=None, body=None):
        super().__init__(message)
        self.stage, self.cls = stage, cls
        self.message, self.http_status, self.body = message, http_status, body

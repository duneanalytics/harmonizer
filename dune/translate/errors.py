class DuneTranslationError(Exception):
    def __init__(self, detail: str):
        self.detail = detail

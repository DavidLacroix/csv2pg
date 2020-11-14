class CsvException(Exception):
    pass


class TooManyFieldsException(CsvException):
    pass


class MissingFieldsException(CsvException):
    pass


class WrongFieldDialectException(CsvException):
    pass

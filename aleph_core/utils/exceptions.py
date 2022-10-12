import traceback


class Exceptions:

    # Connections
    class InvalidKey(Exception): pass
    class ConnectionNotOpen(Exception): pass
    class ConnectionReadingTimeout(Exception): pass
    class ConnectionOpeningTimeout(Exception): pass
    class ConnectionWritingTimeout(Exception): pass

    # Services
    class ServiceInitError(Exception): pass

    # Other
    class InvalidDate(Exception): pass
    class InvalidModel(Exception): pass
    class InvalidRecord(Exception): pass


class Error:

    def __init__(self, exception, **kwargs):
        self.exception = exception
        self.args = kwargs

    @property
    def title(self):
        return repr(self.exception)

    @property
    def message(self):
        args_str = "".join([str(x).title() + ": " + str(self.args[x]) + "\n" for x in self.args])
        if args_str != "": return repr(self.exception) + "\n" + args_str
        return repr(self.exception)

    @property
    def traceback(self):
        return traceback.format_exc()

    @property
    def message_and_traceback(self):
        return self.message + "\n\n" + self.traceback

    @property
    def raise_exception(self):
        raise self.exception

    def __str__(self):
        return self.message_and_traceback

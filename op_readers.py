import cPickle


def file_ops_reader(filename):
    """A naive generator that reads ops from file and returns a doc each time
    Note: it ISN'T thread-safe.
    """
    with open(filename, "rb") as f:
        try:
            while True:
                op = cPickle.load(f)
                yield op
        except EOFError:
            raise StopIteration

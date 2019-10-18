def incrementer(increment=1):

    value = increment

    def _incrementer():
        return value

    return _incrementer
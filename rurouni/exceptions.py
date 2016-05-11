# coding: utf-8


class RurouniException(Exception):
    pass

class ConfigException(RurouniException):
    pass

class TokenBucketFull(RurouniException):
    pass
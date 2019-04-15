from cdiserrors import *



class CustomException(APIError):
    def __init__(self, message):
        self.message = str(message)
        self.code = 500

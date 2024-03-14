from cryptography.fernet import Fernet
from flask import current_app


class cryptography:
    def __init__(self, **kwargs):
        pass

    def load_key(self):
        return b"QnAods0i58its72JKiDgjPPLDrJuwp5eM3fZdEwiQ8s="

    def encrypt(self, value):
        try:
            message = value.encode()
            f = Fernet(self.load_key())
            return f.encrypt(message).decode("utf-8")
        except Exception:
            return value

    def decrypt(self, value):
        try:
            f = Fernet(self.load_key())
            return f.decrypt(bytes(value.encode("utf-8"))).decode()
        except Exception:
            return value
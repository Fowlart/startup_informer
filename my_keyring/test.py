import keyring as kr

class TestKeyring(kr.backend.KeyringBackend):
    """A test keyring which always outputs the same password
    """
    priority = 1

    def set_password(self, servicename, username, password):
        pass

    def get_password(self, servicename, username):
        return "password from TestKeyring"

    def delete_password(self, servicename, username):
        pass

kr.set_keyring(TestKeyring())
kr.set_password("telegram","artur","8888")
print(kr.get_password("telegram","artur"))


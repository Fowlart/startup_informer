import keyring as kr
kr.set_password("telegram","artur","8888")
print(kr.get_password("telegram","artur"))


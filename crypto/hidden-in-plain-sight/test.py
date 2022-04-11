from base64 import b64decode
from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import unpad

key = b64decode("QE1jUWZUalduWnI0dTd4IUElRCpHLUphTmRSZ1VrWHA=".encode('utf-8'))
iv = b64decode("zTnn5Yv9aHjVIhHX2BetXQ==".encode('utf-8'))
ciphered_data = b'nbXg75/acDR47Zgtho29ZVnHqFb7Ikca2SNCWj9SNNe1M+J22JxBrg94feT3anuIx2dQusjf1HJ4fRamU2xGUmHL/Sctgx0ZOsSbIyuksblsjNPmajhzTpljIY0ztR/f6LH5Iq6XJ3MjpTnp4wNg4ODQXfgjyc+UPfk91le4/zIFyAMISCskjw1OYGAOHoS5'
cipher = AES.new(key, AES.MODE_CBC, iv=iv)
original_data = cipher.decrypt(b64decode(ciphered_data))
print(original_data)

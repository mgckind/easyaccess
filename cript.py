from Crypto.Cipher import AES
import hashlib


passwd='matt'
key = hashlib.sha256(passwd).digest()
IV = 16 * '\x00'
mode = AES.MODE_CBC
encryptor = AES.new(key, mode, IV=IV)

text = 'matias,Alnilam1,'
ctext = encryptor.encrypt(text)

decryptor = AES.new(key, mode, IV=IV)
plain = decryptor.decrypt(ctext)



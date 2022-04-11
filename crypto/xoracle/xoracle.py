import binascii

flag_key = b'e1a2014cf2a94c4f2d75e2baa14f995d60dc6b9e161bd11ecf24bad183b80e968a267c3a823e'
flag = b'6a6374667b315f746830553968545f31745f7734355f3533437572655f61303762386130317d'
as_ = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
as_arr = bytes(as_, 'utf-8')

flag_key_bin = int(flag_key, base=16)
flag_bin = int(flag, base=16)
key_bin = flag_key_bin ^ flag_bin

as_arr_bin = int(as_arr, base=16)
as_bin = as_arr_bin ^ key_bin

print(hex(key_bin))
print(hex(as_bin))

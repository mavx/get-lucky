"""
Copied from https://github.com/Shultzi/Mybitcoin/blob/master/Keys_python3Plus
"""

import binascii
import hashlib
import base58
import ecdsa
import os

class Keys():
    def random_key(self):
        random_key = str(binascii.hexlify(os.urandom(32)), 'utf-8')
        return bytes.fromhex(random_key)

    def get_signing_key(self, random_key):
        skey = ecdsa.SigningKey.from_string(random_key, curve=ecdsa.SECP256k1)
        # print('Signing Key:', skey.to_string().hex())
        return skey

    def get_public_key(self, signing_key):
        verifying_key = signing_key.get_verifying_key()
        pkey = bytes.fromhex('04') + verifying_key.to_string()
        # print('Public Key:', pkey.hex())
        return pkey

    def public_key_hash(self, public_key):
        sha256_1 = hashlib.sha256(public_key)
        ripemd160 = hashlib.new("ripemd160")
        ripemd160.update(sha256_1.digest())
        hashed_pkey = bytes.fromhex('00') + ripemd160.digest()
        # print('Public Key Hash:', hashed_pkey.hex())
        return hashed_pkey
    
    def get_checksum(self, hashed_pkey):
        checksum_full = hashlib.sha256(hashlib.sha256(hashed_pkey).digest()).digest()
        return checksum_full[:4]

    def get_bin_addr(self, hashed_public_key, checksum):
        bin_addr = hashed_public_key + checksum
        # print('Bin Address:', bin_addr.hex())
        return bin_addr

    def get_address(self, bin_addr):
        return base58.b58encode(bin_addr)

    def generate(self):
        signing_key = self.get_signing_key(self.random_key())
        public_key = self.get_public_key(signing_key)
        hashed_public_key = self.public_key_hash(public_key)
        checksum = self.get_checksum(hashed_public_key)
        bin_addr = self.get_bin_addr(hashed_public_key, checksum)
        # return self.get_address(bin_addr), public_key.hex(), signing_key
        return {
            'public_key': self.get_address(bin_addr),
            'private_key': signing_key
        }


if __name__ == '__main__':
    k = Keys()
    print('Bitcoin Address:', k.generate())

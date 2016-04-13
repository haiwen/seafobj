#coding: utf-8

from ctypes import (
    create_string_buffer, CDLL, c_char_p,
    c_void_p, c_int, POINTER, byref
)
from ctypes.util import find_library
from seafobj.exceptions import SeafCryptoException

libname = find_library('crypto')
if libname is None:
    raise OSError("Cannot find OpenSSL crypto library")

dl = CDLL(libname)

# EVP_CIPHER_CTX *EVP_CIPHER_CTX_new(void);
EVP_CIPHER_CTX_new = dl.EVP_CIPHER_CTX_new
EVP_CIPHER_CTX_new.restype = c_void_p
EVP_CIPHER_CTX_new.argtypes = []

EVP_aes_256_cbc = dl.EVP_aes_256_cbc
EVP_aes_256_cbc.restype = c_void_p
EVP_aes_256_cbc.argtypes = []

# int EVP_EncryptInit_ex(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *cipher, ENGINE *impl, const unsigned char *key, const unsigned char *iv);
EVP_EncryptInit_ex = dl.EVP_EncryptInit_ex
EVP_EncryptInit_ex.restype = c_int
EVP_EncryptInit_ex.argtypes = [c_void_p, c_void_p, c_void_p, c_char_p, c_char_p]

# int EVP_EncryptUpdate(EVP_CIPHER_CTX *ctx, unsigned char *out, int *outl, const unsigned char *in, int inl);
EVP_EncryptUpdate = dl.EVP_EncryptUpdate
EVP_EncryptUpdate.restype = c_int
EVP_EncryptUpdate.argtypes = [c_void_p, c_char_p, POINTER(c_int), c_char_p, c_int]

# int EVP_EncryptFinal_ex(EVP_CIPHER_CTX *ctx, unsigned char *out, int *outl);
EVP_EncryptFinal_ex = dl.EVP_EncryptFinal_ex
EVP_EncryptFinal_ex.restype = c_int
EVP_EncryptFinal_ex.argtypes = [c_void_p, c_char_p, POINTER(c_int)]

# int EVP_DecryptInit_ex(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *type, ENGINE *impl, unsigned char *key, unsigned char *iv);
EVP_DecryptInit_ex = dl.EVP_DecryptInit_ex
EVP_DecryptInit_ex.restype = c_int
EVP_DecryptInit_ex.argtypes = [c_void_p, c_void_p, c_void_p, c_char_p, c_char_p]

# int EVP_DecryptUpdate(EVP_CIPHER_CTX *ctx, unsigned char *out, int *outl, unsigned char *in, int inl);
EVP_DecryptUpdate = dl.EVP_DecryptUpdate
EVP_DecryptUpdate.restype = c_int
EVP_DecryptUpdate.argtypes = [c_void_p, c_char_p, POINTER(c_int), c_char_p, c_int]

# int EVP_DecryptFinal_ex(EVP_CIPHER_CTX *ctx, unsigned char *outm, int *outl);
EVP_DecryptFinal_ex = dl.EVP_DecryptFinal_ex
EVP_DecryptFinal_ex.restype = c_int
EVP_DecryptFinal_ex.argtypes = [c_void_p, c_char_p, POINTER(c_int)]

#  void EVP_CIPHER_CTX_free(EVP_CIPHER_CTX *ctx);
EVP_CIPHER_CTX_free = dl.EVP_CIPHER_CTX_free
EVP_CIPHER_CTX_free.restype = None
EVP_CIPHER_CTX_free.argtypes = [c_void_p]

class SeafCrypto(object):
    def __init__(self, key, iv):
        self.key = key
        self.iv = iv

    def enc_data(self, data):
        if not data:
            raise SeafCryptoException('Invalid encrypted data')

        ctx = EVP_CIPHER_CTX_new()
        if not ctx:
            raise SeafCryptoException('Failed to create cipher ctx')

        try:
            if EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), None,
                                  self.key, self.iv) == 0:
                raise SeafCryptoException('Failed to init cipher ctx')

            out = create_string_buffer(len(data) + 16)
            out_len = c_int(0)
            if EVP_EncryptUpdate(ctx, out, byref(out_len),
                                 data, len(data)) == 0:
                raise SeafCryptoException('Failed to encrypt update')

            out_final = create_string_buffer(16)
            out_final_len = c_int(0)
            if EVP_EncryptFinal_ex(ctx, out_final,
                                   byref(out_final_len)) == 0:
                raise SeafCryptoException('Failed to encrypt final')

            return out.raw[:out_len.value] + out_final.raw[:out_final_len.value]
        finally:
            EVP_CIPHER_CTX_free(ctx)

    def dec_data(self, data):
        if not data or len(data) % 16 != 0:
            raise SeafCryptoException('Invalid decrypted data')

        ctx = EVP_CIPHER_CTX_new()
        if not ctx:
            raise SeafCryptoException('Failed to create cipher ctx')

        try:
            if EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), None,
                                  self.key, self.iv) == 0:
                raise SeafCryptoException('Failed to init cipher ctx')

            out = create_string_buffer(len(data))
            out_len = c_int(0)
            if EVP_DecryptUpdate(ctx, out, byref(out_len),
                                 data, len(data)) == 0:
                raise SeafCryptoException('Failed to decrypt update')

            out_final = create_string_buffer(16)
            out_final_len = c_int(0)
            if EVP_DecryptFinal_ex(ctx, out_final,
                                   byref(out_final_len)) == 0:
                raise SeafCryptoException('Failed to decrypt final')

            return out.raw[:out_len.value] + out_final.raw[:out_final_len.value]
        finally:
            EVP_CIPHER_CTX_free(ctx)

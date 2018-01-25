from ...base import BaseTest
from ...utils import random_string

from seafobj.utils.crypto import SeafCrypto


class TestSeafCrypto(BaseTest):
    def setUp(self):
        self.key = random_string(20)
        self.iv = random_string(20)
        self.seafCrypto = SeafCrypto(self.key, self.iv)

    def tearDown(self):
        pass

    def test_enc_data(self):
        data = random_string(100)
        res = self.seafCrypto.enc_data(data)
        self.assertNotEqual(data, res)
        self.assertEqual(data, self.seafCrypto.dec_data(res))

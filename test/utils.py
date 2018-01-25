import string
import random

def random_string(length=1): return ''.join(random.choice(string.lowercase) for i in range(length))

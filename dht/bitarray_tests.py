import bitarray
import ctypes
from bitarray import util
import random
from collections import deque, defaultdict

rint = random.randint(0, 99)
inthash = hash(hex(rint))

barray = util.int2ba(ctypes.c_ulong(inthash).value, length=64)
print(barray)
print(len(barray))
print(barray.to01())

defD = {'b': 1, 'g': 2, 'r': 3, 'y': 4}
d = defaultdict()
for i, v in defD.items():
    d[i] = v

print(d.items())
q = deque(d.items())
print(q[2:])


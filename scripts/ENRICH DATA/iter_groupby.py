from itertools import groupby

l = [('key1', 1),('key1', 2),('key1', 3),('key2', 4),('key2', 5)]
l.sort(key = lambda i : i[0])

[{key: [i[1] for i in values]} for key, values in groupby(l, lambda i: i[0])]
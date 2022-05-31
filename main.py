import sys

if __name__ == "__main__":
    print(f"Arguments count: {len(sys.argv)}")
    for i, arg in enumerate(sys.argv):
        print(f"Argument {i:>6}: {arg}")


test = [("a", 5), ("b", 3), ("c", 7)]
test2 = {
    "a": 15,
    "c": 35
}

def testando(a, b):
    b = [e for e in b if e[0] in a]

for k in test2:
    test2[k] += 10


print(test2)
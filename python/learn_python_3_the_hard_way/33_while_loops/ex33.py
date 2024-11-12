i = 0
numbers = []


def inner_loop(n, p):
    print(f"At the top i is {n}")
    numbers.append(n)
    n = n + p
    print("Numbers now: ", numbers)
    return n


while i < 6:
    i = inner_loop(i, 1)

print(f"At the bottom i is {i}")

print("The numbers: ")
for num in numbers:
    print(num)

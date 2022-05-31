from math import sqrt, pow

def histogram(words: list[str]) -> dict[str, int]:
    result = {}

    for word in words:
        if (word not in result): 
            result[word] = 0
        result[word] += 1

    return result

def calcNormIndex(values: list[tuple[int,float]]) -> list[tuple[int,float]]:
    result: dict[int, float] = {}
    for item in values:
        if (item[0] not in result):
            result[item[0]] = 0
        result[item[0]] += pow(item[1], 2)
    return [(k, sqrt(result[k])) for k in result]
from typing import Callable
from statistics import mean
import nltk

nltk.download('punkt')
nltk.download('stopwords')

stopwords = nltk.corpus.stopwords.words('english')

def tokenizeFile(content: str) -> list[str]:
    tokenizer = nltk.RegexpTokenizer(r"[A-Aa-z]{3,}")
    return removeStopwords(tokenizer.tokenize(content))

def removeStopwords(words: list[str]) -> list[str]:
    return [w for w in words if w not in stopwords]

def stemWords(words: list[str]) -> list[str]:
    stemmer = nltk.stem.PorterStemmer()
    return [stemmer.stem(w) for w in words]

def iqqFilter(terms: dict[str, int], fileterms: list[tuple[str, int, int]], iqq: float) -> list[tuple[str, int, int]]:
    mean_value = round(mean([terms[e] for e in terms if terms[e] != 1]), None)
    max_value = round(mean_value * (mean_value / iqq), None)
    min_value = round(iqq, None)
    bkp_terms = dict(list(terms.items()))
    for e in bkp_terms:
        if (terms[e] < min_value or terms[e] > max_value):
            del terms[e]
    return [e for e in fileterms if e[0] in terms]
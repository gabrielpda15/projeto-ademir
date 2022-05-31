from typing import Callable
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

def iqqFilter(terms: dict[str, int], fileterms: list[tuple[str, int, int]], filter: Callable[[str, int],bool]) -> list[tuple[str, int, int]]:
    for e in terms:
        if (not filter(e, terms[e])):
            del terms[e]
    return [e for e in fileterms if e[0] in terms]
import nltk

nltk.download('punkt')
nltk.download('stopwords')

stopwords = nltk.corpus.stopwords.words('english')

def tokenizeFile(content: str) -> list[str]:
    tokenizer = nltk.RegexpTokenizer(r"\w+")
    return removeStopwords(tokenizer.tokenize(content))

def removeStopwords(words: list[str]) -> list[str]:
    return [w for w in words if w not in stopwords]
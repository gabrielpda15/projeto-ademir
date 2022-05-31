class File:
    id: int
    code: str
    folder: str
    type: str
    size: int
    norm_index: float

    def __init__(self, code: str, folder: str, type: str, size: int):
        self.id = 0
        self.norm_index = 0
        self.code = code
        self.folder = folder
        self.type = type
        self.size = size

class Term:
    id: int
    term: str
    count: int
    idf: float

    def __init__(self, term: str):
        self.id = 0
        self.count = 0
        self.idf = 0
        self.term = term

class FileTerm:
    term_id: int
    file_id: int
    count: int
    tf: float
    tfidf: float

    def __init__(self, file_id: int, term_id: int, count: int):        
        self.tf = 0
        self.tfidf = 0
        self.file_id = file_id
        self.term_id = term_id
        self.count = count

class Similarity:
    query_id: int
    dataset_id:int
    value: float

    def __init__(self, query_id: int, dataset_id: int, value: float):
        self.query_id = query_id
        self.dataset_id = dataset_id
        self.value = value

def createFileTerm(data: tuple[int, int, int, float, float]) -> FileTerm:
    obj = FileTerm(data[0], data[1], data[2])
    obj.tf = data[3]
    obj.tfidf = data[4]
    return obj

def createFile(data: tuple[str, str, str, int, int, float]) -> File:
    obj = File(data[0], data[1], data[2], data[3])
    obj.id = data[4]
    obj.norm_index = data[5]
    return obj

def createTerm(data: tuple[int, str, int, float]) -> File:
    obj = Term(data[1])
    obj.id = data[0]
    obj.count = data[2]
    obj.idf = data[3]
    return obj

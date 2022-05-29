from glob import glob
from datetime import datetime, timedelta
from models import FileTerm

def getFilesInFolder(folder: str) -> list[tuple[str, str]]:
    dataset = glob(f".\\dataset\\{folder}\\*.txt")
    query = glob(f".\\query\\{folder}\\*.txt")
    result = {}

    for e in dataset:
        result[e] = 'dataset'

    for e in query:
        result[e] = 'query'
        
    return list(result.items())

def getElapsedTime(time: timedelta) -> str: 
    return f"{int(time.total_seconds())}s{int((time.total_seconds() - time.seconds) * 1000)}ms"

def find(term_id: int, values: list[FileTerm]) -> FileTerm:
    for item in values:
        if (item.term_id == term_id):
            return item
    return None

def log_header(severity: str) -> str:
    log_message = "[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] "
    log_message += severity.upper().rjust(6) + " : "
    return log_message

def log(message: str, severity: str) -> None:
    temp = log_header(severity)
    print(temp + message)
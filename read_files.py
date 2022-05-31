from datetime import datetime
from glob import glob
from sys import exit
from library.database_utils import Database
from library.models import File, Term, FileTerm
from library.utils import log, log_header, getFilesInFolder, getElapsedTime
from library.nltk_utils import iqqFilter, stemWords, tokenizeFile
from library.maths import histogram
from progress.bar import Bar

def execute(database_name: str, encoding: str = 'utf8', extension: str = '.txt') -> None:
    database = Database(database_name)
    db_status = database.ensureCreate()
    if not db_status:
        exit(1)

    time = datetime.now()

    all_folders = [(e.split('\\')[-2]) for e in glob(".\\dataset\\*\\")]
    progress_bar = Bar(max=len(glob(f".\\dataset\\**\\*{extension}")) + len(glob(f".\\query\\**\\*{extension}")))

    terms: dict[str, int] = {}
    fileterms: list[tuple[str, int, int]] = []

    for folder in all_folders:
        for file_path in getFilesInFolder(folder):
            progress_bar.message = log_header("INFO") + 'Reading files'
            progress_bar.next()
            with open(file_path[0], 'rb') as file:
                file_name = file_path[0].split('\\')[-1].split('.')[0]
                binary_content = file.read()
                content = binary_content.decode(encoding).lower()
                file_obj = File(file_name, folder, file_path[1], len(binary_content))
                file_id = database.createFile(file_obj)
                tokenized_file = tokenizeFile(content)
                stemmed_file = stemWords(tokenized_file)
                histogramed_file = histogram(stemmed_file)

                for w in histogramed_file:
                    if (w not in terms):
                        terms[w] = 0
                    terms[w] += histogramed_file[w]
                    fileterms.append((w, file_id, histogramed_file[w]))

    fileterms = iqqFilter(terms, fileterms, (lambda word, count: True))

    database.createTerms(list(terms.items()))
    all_terms = dict([(e.term, e.id) for e in database.getTerms()])

    fileterms_objs = [FileTerm(e[1], all_terms[e[0]], e[2]) for e in fileterms]
    fileterms_objs_result: dict[str, FileTerm] = {}
    for obj in fileterms_objs:
        key = f"{obj.file_id}:{obj.term_id}"
        if (key not in fileterms_objs_result):
            fileterms_objs_result[key] = FileTerm(obj.file_id, obj.term_id, 0)
        fileterms_objs_result[key].count += obj.count
    database.createFileTerms(list(fileterms_objs_result.values()))
        
    progress_bar.finish()
        
    log(f"Done reading all files! {getElapsedTime(datetime.now() - time)}", "INFO")
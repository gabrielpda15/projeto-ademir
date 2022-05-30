from datetime import datetime
from glob import glob
from sys import exit
from library.database_utils import Database
from library.models import File, Term, FileTerm
from library.utils import log, log_header, getFilesInFolder, getElapsedTime
from library.nltk_utils import tokenizeFile
from library.maths import histogram
from progress.bar import Bar

encoding = 'utf8'

database = Database('database.db')
db_status = database.ensureCreate()
if not db_status:
    exit(1)

time = datetime.now()

all_folders = [(e.split('\\')[-2]) for e in glob(".\\dataset\\*\\")]
progress_bar = Bar(max=len(glob(".\\dataset\\**\\*.txt")) + len(glob(".\\query\\**\\*.txt")))
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
            histogramed_file = histogram(tokenizeFile(content))            
            for word in histogramed_file:
                term = Term(word)
                term_id = database.updateTerm(term, histogramed_file[word])
                file_term = FileTerm(file_id, term_id, histogramed_file[word])
                file_term_id = database.createFileTerm(file_term)
progress_bar.finish()
    
log(f"Done reading all files! {getElapsedTime(datetime.now() - time)}", "INFO")
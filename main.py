from datetime import datetime
from glob import glob
from sys import exit
from database_utils import Database
from models import File, Term, FileTerm, Similarity
from utils import log, log_header, getFilesInFolder, getElapsedTime, find
from nltk_utils import tokenizeFile
from maths import histogram, calcNormIndex
from math import log2
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

time = datetime.now()

log("Calculating term frequency...", "INFO")
all_fileterms = database.getFileTerms()
progress_bar = Bar(max = len(all_fileterms))
for item in all_fileterms:
    progress_bar.message = log_header("INFO") + 'Calculating'
    progress_bar.next()
    item.tf = 1 + log2(item.count)
    database.updateTermFrequency(item)
progress_bar.finish()

log("Calculating inverse document frequency...", "INFO")

n_files = database.getNumberOfFiles()
terms_ids = database.getTermsIds()
progress_bar = Bar(max = len(terms_ids))
for term_id in terms_ids:
    progress_bar.message = log_header("INFO") + 'Calculating'
    progress_bar.next()
    n = database.getFileTermsCount(term_id)
    database.updateInverseDocFrequency(term_id, log2(n_files / n))
progress_bar.finish()

log("Calculating TF-IDF...", "INFO")

db_status = database.defineTFIDF()
if not db_status:
    exit(1)

log("Calculating Norm Index...", "INFO")

progress_bar = Bar(max = 3)
progress_bar.message = log_header("INFO") + 'Getting TF-IDF'
progress_bar.next()
all_tfidf = database.getAllTFIDF()
progress_bar.message = log_header("INFO") + 'Calculating norm index'
progress_bar.next()
all_tfidf = calcNormIndex(all_tfidf)
progress_bar.message = log_header("INFO") + 'Updating database'
progress_bar.next()
db_status = database.updateNormIndex(all_tfidf)
if not db_status:
    exit(1)
progress_bar.finish()

log("Calculating the similarity...", "INFO")

all_fileterms = database.getFileTerms()
all_dataset_files = database.getFiles("dataset")
all_query_files = database.getFiles("query")

progress_bar = Bar(max = len(all_query_files) * len(all_dataset_files))

for query in all_query_files:
    for dataset in all_dataset_files:
        progress_bar.message = log_header("INFO") + 'Calculating similarity'
        progress_bar.next()
        if (query.folder == dataset.folder):
            query_terms = [e for e in all_fileterms if e.file_id == query.id]
            dataset_terms = [e for e in all_fileterms if e.file_id == dataset.id]
            result = 0
            for query_term in query_terms:
                dataset_term = find(query_term.term_id, dataset_terms)
                if (dataset_term):
                    result += query_term.tfidf * dataset_term.tfidf
            denominator = query.norm_index * dataset.norm_index
            if (denominator == 0):
                result = 0
            else:
                result = result / denominator
            similarity = Similarity(query.id, dataset.id, result)
            database.createSimilarity(similarity)
progress_bar.finish()

log(f"Done! {getElapsedTime(datetime.now() - time)}", "INFO")

























    
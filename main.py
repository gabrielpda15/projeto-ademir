from datetime import datetime
from glob import glob
from sys import exit
from database_utils import Database
from models import File, Term, FileTerm, Similarity
from utils import log, getFilesInFolder, getElapsedTime, find
from nltk_utils import tokenizeFile
from maths import histogram, calcNormIndex
from math import log2

encoding = 'utf8'

database = Database('database.db')
db_status = database.ensureCreate()
if not db_status:
    exit(1)

time = datetime.now()

for folder in [(e.split('\\')[-2]) for e in glob(".\\dataset\\*\\")]:
    log(f"Reading files from './{folder}'", "INFO")
    for file_path in getFilesInFolder(folder):
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
    
log(f"Done reading all files! {getElapsedTime(datetime.now() - time)}", "INFO")

time = datetime.now()

log("Calculating term frequency...", "INFO")

for item in database.getFileTerms():
    item.tf = 1 + log2(item.count)
    database.updateTermFrequency(item)

log("Calculating inverse document frequency...", "INFO")

n_files = database.getNumberOfFiles()
terms_ids = database.getTermsIds()

for term_id in terms_ids:
    n = database.getFileTermsCount(term_id)
    database.updateInverseDocFrequency(term_id, log2(n_files / n))

log("Calculating TF-IDF...", "INFO")

db_status = database.defineTFIDF()
if not db_status:
    exit(1)

log("Calculating Norm Index...", "INFO")

all_tfidf = database.getAllTFIDF()
all_tfidf = calcNormIndex(all_tfidf)
db_status = database.updateNormIndex(all_tfidf)
if not db_status:
    exit(1)

log("Calculating the similarity...", "INFO")

all_fileterms = database.getFileTerms()
all_dataset_files = database.getFiles("dataset")
all_query_files = database.getFiles("query")

for query in all_query_files:
    for dataset in all_dataset_files:
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

log(f"Done! {getElapsedTime(datetime.now() - time)}", "INFO")

























    
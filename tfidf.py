from datetime import datetime
from sys import exit
from library.database_utils import Database
from library.utils import getElapsedTime, log, log_header
from library.maths import calcNormIndex
from math import log2
from progress.bar import Bar

database = Database('database.db')
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

log(f"Done! {getElapsedTime(datetime.now() - time)}", "INFO")
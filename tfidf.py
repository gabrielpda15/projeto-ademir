from datetime import datetime
from sys import exit
from library.database_utils import Database
from library.utils import getElapsedTime, log, log_header
from library.maths import calcNormIndex
from math import log2
from progress.bar import Bar

def execute(database_name: str) -> None:
    database = Database(database_name)
    time = datetime.now()

    log("Calculating term frequency...", "INFO")
    all_fileterms = database.getFileTerms()
    progress_bar = Bar(max = len(all_fileterms))
    for item in all_fileterms:
        progress_bar.message = log_header("INFO") + 'Calculating'
        progress_bar.next()
        item.tf = 1 + log2(item.count)
    progress_bar.finish()
    log("Saving term frequency...", "INFO")
    database.updateTermFrequencies(all_fileterms)

    all_fileterms.clear()

    log("Calculating inverse document frequency...", "INFO")

    n_files = database.getNumberOfFiles()
    terms_ids = database.getTermsIds()
    n_files_per_term = database.getFileTermsCount()
    idf_result: dict[int, float] = {}
    progress_bar = Bar(max = len(terms_ids))
    for term_id in terms_ids:
        progress_bar.message = log_header("INFO") + 'Calculating'
        progress_bar.next()
        idf_result[term_id] = log2(1 + (n_files / n_files_per_term[term_id]))
    progress_bar.finish()
    log("Saving inverse document frequency...", "INFO")
    database.updateInverseDocFrequencies(idf_result)

    terms_ids.clear()
    n_files_per_term.clear()
    idf_result.clear()

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
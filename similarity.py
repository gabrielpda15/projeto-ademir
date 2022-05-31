from datetime import datetime
from library.database_utils import Database
from library.models import Similarity
from library.utils import log, log_header, getElapsedTime, find
from progress.bar import Bar

def execute(database_name: str) -> None:
    database = Database(database_name)
    time = datetime.now()

    log("Calculating the similarity...", "INFO")

    all_fileterms = database.getFileTerms()
    all_dataset_files = database.getFiles("dataset")
    all_query_files = database.getFiles("query")

    progress_bar = Bar(max = len(all_query_files) * len(all_dataset_files))

    for query in all_query_files:
        similarity_result = []
        for dataset in all_dataset_files:
            progress_bar.message = log_header("INFO") + 'Calculating similarity'
            progress_bar.next()
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
            similarity_result.append(similarity)
        database.createSimilarities(similarity_result)
    progress_bar.finish()

    log(f"Done! {getElapsedTime(datetime.now() - time)}", "INFO")
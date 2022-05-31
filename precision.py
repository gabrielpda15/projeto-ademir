from datetime import datetime
from library.database_utils import Database
from os import path, makedirs
from itertools import islice
from progress.bar import Bar
from library.utils import getElapsedTime, log_header, log

precisions = [ 10, 20, 50, 100 ]

database = Database('database.db')
time = datetime.now()

if (not path.exists(".\\output")):
    makedirs(".\\output")

log("Getting all queries...", "INFO")

all_queries = database.getFiles("query")
progress_bar = Bar(max = len(all_queries))

log("Calculating precision and recall...", "INFO")

for query in all_queries:
    progress_bar.message = log_header("INFO") + 'Calculating'
    progress_bar.next()

    answer_set = database.getAnswerSet(query.id)
    relevant_docs = database.getRelevantDocs(query.id, query.folder)
    intersection = []
    for answer in answer_set:
        for doc in relevant_docs:
            if (answer[0] == doc[0]):
                intersection.append(doc)
    d = len(relevant_docs)
    recall = 0
    precision = 0
    if (d != 0):        
        recall = len(intersection) / d
    d = len(answer_set)
    if (d != 0):
        precision = len(intersection) / d

    database.updatePrecisionRecall(query.id, recall, precision)

    for p in precisions:
        out_path = f".\\output\\p{p}\\{query.folder}"
        if (not path.exists(out_path)):
            makedirs(out_path)
        with open(out_path + f"\\{query.code}.txt", "wt") as file:
            sliced_set = islice(answer_set, p)
            n_relevant = 0
            for sim in sliced_set:
                relevant_mark: str = '   '
                for doc in relevant_docs:
                    if (sim[0] == doc[0]):
                        relevant_mark = ' * '
                        n_relevant += 1
                file.write(f"{relevant_mark}{sim[2]}\\{sim[1]}.txt = {round(sim[3], 4)}\n")
            file.write(f"\np@{p} = {round(n_relevant * 100 / p, 2)}%")
            file.flush()
progress_bar.finish()          
log(f"Done! {getElapsedTime(datetime.now() - time)}", "INFO")
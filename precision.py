from datetime import datetime
from library.database_utils import Database
from os import path, makedirs
from itertools import islice

precisions = [ 10, 20, 50, 100 ]

database = Database('database.db')
time = datetime.now()

if (not path.exists(".\\output")):
    makedirs(".\\output")

all_queries = database.getFiles("query")

for query in all_queries:
    similarities = database.getSimilarity(query.id, max(precisions))

    for p in precisions:
        out_path = f".\\output\\p{p}\\{query.folder}"
        if (not path.exists(out_path)):
            makedirs(out_path)
        with open(out_path + f"\\{query.code}.txt", "wt") as file:
            for sim in islice(similarities, p):
                file.write(f"{sim[1]}\\{sim[0]}.txt = {round(sim[2], 4)}\n")
            file.flush()
            
    


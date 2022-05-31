from datetime import datetime
from library.database_utils import Database
from progress.bar import Bar
from library.utils import getElapsedTime, log_header, log

def execute(database_name: str, extension: str = '.txt') -> None:
    database = Database(database_name)
    time = datetime.now()

    log("Calculating MAPs...", "INFO")

    all_folders = database.getAllFolders()
    progress_bar = Bar(max = len(all_folders) + 1)

    with open(f".\\output\\maps{extension}", "wt") as file:
        progress_bar.message = log_header("INFO") + 'Calculating'
        progress_bar.next()
        general_map = database.getGeneralMap()
        file.write(f"general = {(round(general_map * 100, 3))}%\n\n")

        for folder in database.getAllFolders():
            progress_bar.message = log_header("INFO") + 'Calculating'
            progress_bar.next()
            map = database.getFolderMap(folder)
            file.write(f"{folder} = {(round(map * 100, 3))}%\n")
        file.flush()

    progress_bar.finish()

    log(f"Done! {getElapsedTime(datetime.now() - time)}", "INFO")
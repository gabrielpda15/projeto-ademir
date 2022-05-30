from sqlite3 import Error, connect
from library.utils import log
from library.models import File, Term, FileTerm, Similarity, createFileTerm, createFile

class Database:
    name: str

    def __init__(self, name: str):
        self.name = name
    
    def ensureCreate(self) -> bool:
        connection = connect(self.name)

        try:        
            cur = connection.cursor()

            cur.execute("BEGIN")

            sql = """DROP TABLE IF EXISTS terms"""
            cur.execute(sql)
            sql = """
                CREATE TABLE terms (
                    id INTEGER NOT NULL,
                    term TEXT NOT NULL UNIQUE,
                    count INTEGER DEFAULT 0,                    
                    idf REAL DEFAULT 0,
                    PRIMARY KEY(id)
                )
            """
            cur.execute(sql)

            sql = """DROP TABLE IF EXISTS files"""
            cur.execute(sql)
            sql = """
                CREATE TABLE files (
                    id INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    folder TEXT NOT NULL,
                    type TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    norm_index REAL DEFAULT 0,
                    PRIMARY KEY(id)
                )
            """
            cur.execute(sql)

            sql = """DROP TABLE IF EXISTS file_terms"""
            cur.execute(sql)
            sql = """
                CREATE TABLE file_terms (
                    term_id INTEGER NOT NULL,
                    file_id INTEGER NOT NULL,
                    count INTEGER DEFAULT 0,
                    tf REAL DEFAULT 0,
                    tfidf REAL DEFAULT 0,
                    PRIMARY KEY (term_id, file_id)
                    FOREIGN KEY (term_id) 
                        REFERENCES terms (id) 
                            ON DELETE CASCADE 
                            ON UPDATE NO ACTION
                    FOREIGN KEY (file_id) 
                        REFERENCES files (id) 
                            ON DELETE CASCADE 
                            ON UPDATE NO ACTION
                )
            """
            cur.execute(sql)

            sql = """DROP TABLE IF EXISTS similarities"""
            cur.execute(sql)
            sql = """
                CREATE TABLE similarities (
                    query_id INTEGER NOT NULL,
                    dataset_id INTEGER NOT NULL,
                    value REAL NOT NULL,
                    PRIMARY KEY (query_id, dataset_id)
                    FOREIGN KEY (query_id) 
                        REFERENCES files (id) 
                            ON DELETE CASCADE 
                            ON UPDATE NO ACTION
                    FOREIGN KEY (dataset_id) 
                        REFERENCES files (id) 
                            ON DELETE CASCADE 
                            ON UPDATE NO ACTION
                )
            """
            cur.execute(sql)
            cur.execute("COMMIT")
            log("Successfuly created database!", "INFO")
            return True
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
        finally:
            connection.close()

    def createFile(self, obj: File) -> int:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "INSERT INTO files(code, folder, type, size) VALUES(?,?,?,?)"
            cur.execute(sql, [obj.code, obj.folder, obj.type, obj.size])
            connection.commit()
            return cur.lastrowid
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def updateTerm(self, obj: Term, count: int) -> int:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "INSERT OR IGNORE INTO terms(term) VALUES(?)"
            cur.execute(sql, [obj.term])
            sql = "SELECT id FROM terms WHERE term = ?"
            cur.execute(sql, [obj.term])
            term_id = cur.fetchone()[0]
            sql = "UPDATE terms SET count = count + ? WHERE id = ?"
            cur.execute(sql, [count, term_id])
            connection.commit()
            return term_id
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def createFileTerm(self, obj: FileTerm) -> int:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "INSERT INTO file_terms(term_id, file_id, count) VALUES(?,?,?)"
            cur.execute(sql, [obj.term_id, obj.file_id, obj.count])
            connection.commit()
            return cur.lastrowid
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def getFileTerms(self) -> list[FileTerm]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "SELECT file_id, term_id, count, tf, tfidf FROM file_terms"
            cur.execute(sql)
            return [createFileTerm(e) for e in cur.fetchall()]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return None
        finally:
            connection.close()

    def updateTermFrequency(self, obj: FileTerm) -> FileTerm:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "UPDATE file_terms SET tf = ? WHERE file_id = ? AND term_id = ?"
            cur.execute(sql, [obj.tf, obj.file_id, obj.term_id])
            connection.commit()            
            return obj
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return None
        finally:
            connection.close()

    def updateInverseDocFrequency(self, term_id: int, idf: int) -> int:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "UPDATE terms SET idf = ? WHERE id = ?"
            cur.execute(sql, [idf, term_id])
            connection.commit()            
            return term_id
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def getNumberOfFiles(self) -> int:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "SELECT count(*) FROM files"
            cur.execute(sql)
            return cur.fetchone()[0]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def getTermsIds(self) -> list[int]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "SELECT id FROM terms"
            cur.execute(sql)
            return [e[0] for e in cur.fetchall()]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return None
        finally:
            connection.close()

    def getFileTermsCount(self, term_id: int) -> int:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "SELECT count(*) FROM file_terms WHERE term_id = ?"
            cur.execute(sql, [term_id])
            return cur.fetchone()[0]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def defineTFIDF(self) -> bool:        
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = """
                UPDATE file_terms
                SET tfidf = q.tfidf
                FROM (
                    SELECT f.file_id, f.term_id, f.tf * t.idf as tfidf
                    FROM terms t
                    INNER JOIN file_terms f ON f.term_id = t.id
                ) q
                WHERE file_terms.file_id = q.file_id AND file_terms.term_id = q.term_id
            """
            cur.execute(sql)
            connection.commit()
            return True
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
        finally:
            connection.close()

    def getAllTFIDF(self) -> list[tuple[int,float]]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = """
                SELECT file_id, tfidf
                FROM file_terms
            """
            cur.execute(sql)
            return cur.fetchall()
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return None
        finally:
            connection.close()


    def updateNormIndex(self, values: list[tuple[int,float]]) -> bool:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            for item in values:
                sql = "UPDATE files SET norm_index = ? WHERE id = ?"
                cur.execute(sql, [item[1], item[0]])
            connection.commit()
            return True
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
        finally:
            connection.close()

    def getFiles(self, type: str) -> list[File]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "SELECT code, folder, type, size, id, norm_index FROM files WHERE type = ?"
            cur.execute(sql, [type])
            return [createFile(e) for e in cur.fetchall()]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return None
        finally:
            connection.close()

    def createSimilarity(self, obj: Similarity) -> int:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "INSERT INTO similarities(query_id, dataset_id, value) VALUES(?,?,?)"
            cur.execute(sql, [obj.query_id, obj.dataset_id, obj.value])
            connection.commit()
            return cur.lastrowid
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def getSimilarity(self, query_id: int, count: int) -> list[tuple[str, str, float]]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = """
                SELECT f.code, f.folder, s.value FROM similarities s
                INNER JOIN files f ON f.id = s.dataset_id
                WHERE s.query_id = ?
                ORDER BY s.value DESC 
                LIMIT ?
            """
            cur.execute(sql, [query_id, count])
            return cur.fetchall()
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()
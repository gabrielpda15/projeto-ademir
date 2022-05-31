from sqlite3 import Error, connect
from library.utils import log
from library.models import File, Term, FileTerm, Similarity, createFileTerm, createFile, createTerm

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
                    recall REAL DEFAULT 0,
                    precision REAL DEFAULT 0,
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

    def createTerms(self, values: list[tuple[str,int]]) -> bool:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = f"INSERT OR IGNORE INTO terms (term, count) VALUES "
            sql += ','.join([f'(\'{e[0]}\',{e[1]})' for e in values])
            cur.execute(sql)
            connection.commit()
            return True
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
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

    def createFileTerms(self, values: list[FileTerm]) -> bool:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = f"INSERT INTO file_terms(term_id, file_id, count) VALUES "
            sql += ','.join([f'({e.term_id},{e.file_id},{e.count})' for e in values])
            cur.execute(sql)
            connection.commit()
            return True
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
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

    def updateTermFrequencies(self, values: list[FileTerm]) -> bool:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            temp_values = ','.join([f'({e.file_id},{e.term_id},{e.tf})' for e in values])
            sql = f"""
                WITH tmp(file_id, term_id, tf) AS (
                    VALUES {temp_values}
                )
                UPDATE file_terms SET tf = (
                    SELECT tf
                    FROM tmp
                    WHERE file_terms.file_id = tmp.file_id AND file_terms.term_id = tmp.term_id
                )
                WHERE file_id IN (SELECT file_id FROM tmp) AND term_id IN (SELECT term_id FROM tmp)
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

    def updateInverseDocFrequencies(self, values: dict[int, float]) -> bool:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            temp_values = ','.join([f'({e},{values[e]})' for e in values])
            sql = f"""
                WITH tmp(term_id, idf) AS (
                    VALUES {temp_values}
                )
                UPDATE terms SET idf = (
                    SELECT idf
                    FROM tmp
                    WHERE terms.id = tmp.term_id
                )
                WHERE id IN (SELECT term_id FROM tmp)
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

    def getTerms(self) -> list[Term]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "SELECT id, term, count, idf FROM terms"
            cur.execute(sql)
            return [createTerm(e) for e in cur.fetchall()]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return None
        finally:
            connection.close()

    def getFileTermsCount(self) -> dict[int, int]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "SELECT term_id, count(*) FROM file_terms GROUP BY term_id"
            cur.execute(sql)
            return dict(cur.fetchall())
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
            temp_values = ','.join([f'({e[0]},{e[1]})' for e in values])
            sql = f"""
                WITH tmp(file_id, norm_index) AS (
                    VALUES {temp_values}
                )
                UPDATE files SET norm_index = (
                    SELECT norm_index
                    FROM tmp
                    WHERE files.id = tmp.file_id
                )
                WHERE id IN (SELECT file_id FROM tmp)
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

    def createSimilarities(self, values: list[Similarity]) -> bool:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "INSERT INTO similarities(query_id, dataset_id, value) VALUES "
            sql += ','.join([f'({e.query_id},{e.dataset_id},{e.value})' for e in values])
            cur.execute(sql)
            connection.commit()
            return True
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
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

    def getAnswerSet(self, query_id: int) -> list[tuple[int, str, str, float]]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = """
                SELECT f.id, f.code, f.folder, s.value FROM similarities s
                INNER JOIN files f ON f.id = s.dataset_id
                WHERE s.query_id = ?
                ORDER BY s.value DESC
            """
            cur.execute(sql, [query_id])
            return cur.fetchall()
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def getRelevantDocs(self, query_id: int, folder: str) -> list[tuple[int, str, str, float]]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = """
                SELECT f.id, f.code, f.folder, s.value FROM similarities s
                INNER JOIN files f ON f.id = s.dataset_id
                WHERE s.query_id = ? AND f.folder = ?
                ORDER BY s.value DESC
            """
            cur.execute(sql, [query_id, folder])
            return cur.fetchall()
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return -1
        finally:
            connection.close()

    def updatePrecisionRecall(self, query_id: int, recall: float, precision: float) -> bool:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "UPDATE files SET recall = ?, precision = ? WHERE id = ?"
            cur.execute(sql, [recall, precision, query_id])
            connection.commit()
            return True
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
        finally:
            connection.close()

    def getGeneralMap(self) -> float:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = """
                SELECT sum(precision) / (
                    SELECT count(*)
                    FROM files
                    WHERE type = 'query'
                ) as result
                FROM files
                WHERE type = 'query' AND recall > 0            
            """
            cur.execute(sql)
            return cur.fetchone()[0]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
        finally:
            connection.close()

    def getAllFolders(self) -> list[str]:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = "SELECT folder FROM files GROUP BY folder"
            cur.execute(sql)
            return [e[0] for e in cur.fetchall()]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
        finally:
            connection.close()

    def getFolderMap(self, folder: str) -> float:
        connection = connect(self.name)

        try:
            cur = connection.cursor()
            sql = """
                SELECT sum(precision) / (
                    SELECT count(*)
                    FROM files
                    WHERE type = 'query' AND folder = ?
                ) as result
                FROM files
                WHERE type = 'query' AND folder = ? AND recall > 0
            """
            cur.execute(sql, [folder, folder])
            return cur.fetchone()[0]
        except Error as error:
            log("Something went wrong!", "ERROR")
            log(error.__str__(), "ERROR")
            return False
        finally:
            connection.close()
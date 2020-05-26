# from multiprocessing import Pool, Queue, Lock
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
import os
import sys
import re
import json
from bs4 import BeautifulSoup
import string

from nltk.stem import PorterStemmer

from threading import Thread
from threading import Lock

import hashlib

from collections import defaultdict

from tokenizer import compute_word_frequencies
from tokenizer import tokenize
from Posting import Posting


class Indexer:
    '''
    src_dir is the directory of web contents json file: DEV
    dest_dir is the index directory: Index
    Index should contain directories 0, 1, 2, 3, ... , 9 and a, b, c, ... , z
    In each directory, every single term is a file. (ex.  apple.txt)
    each file contains the posting list of that term, one posting in a line
    '''
    
    lock = Lock()
    
    def __init__(self, src_dir: Path, index_dir: Path, log_dir: Path,
                 batch_size: int):
        self.src_dir = src_dir
        self.index_dir = index_dir
        self.log_dir = log_dir
        self.batch_size = batch_size
    
    def construct_index(self):
        self.create_dir()
        src_paths = self.open_source_dir()
        self.get_docid_to_url(src_paths)
        status = self.get_status_json()
        write_batch_count = status["write_batches"]
        while write_batch_count * self.batch_size < len(src_paths):
            status = self.get_status_json()
            read_batch_count = status["read_batches"]
            write_batch_count = status["write_batches"]
            
            
            if read_batch_count > write_batch_count:
                print(
                    f'Indexing documents {write_batch_count * self.batch_size} ~ '
                    f'{read_batch_count * self.batch_size}')
                self.write_batch(recover=True)
                print(f'Finish Indexing documents'
                      f' {read_batch_count * self.batch_size} ~ '
                      f'{(read_batch_count + 1) * self.batch_size}')
                continue

            print(
                f'Indexing documents {read_batch_count * self.batch_size} ~ '
                f'{(read_batch_count + 1) * self.batch_size}')
            
            partial_index = self.read_batch(src_paths, read_batch_count *
                                            self.batch_size, self.batch_size)
            print("partial index stored")
            self.write_batch(partial_index)
            
            print(f'Finish Indexing documents'
                  f' {read_batch_count * self.batch_size} ~ '
                  f'{(read_batch_count + 1) * self.batch_size}')
        
        print("--------------done !------------------")
    
    '''
    Read in certain number of source json file
    return a inverted index dictionary, key is the term, value is the posting.
    
    Also write partial_index into log_dir
    '''
    
    def read_batch(self, src_files_paths=None, start=0, limit=0) -> dict:
        if src_files_paths is None:
            src_files_paths = list()
        
        partial_index = defaultdict(lambda: list())
        ps = PorterStemmer()
        
        i = start
        if (start + limit) < len(src_files_paths):
            end = start + limit
        else:
            end = len(src_files_paths)
        
        for src_file_path in src_files_paths[start: end]:
            with open(src_file_path, "r", encoding="utf_8") as file:
                json_data = json.load(file)
                content = json_data["content"]
            
            text = BeautifulSoup(content, features="html.parser").get_text()
            
            # do similarity test here, uncompleted
            
            # ------------------------
            
            tokens = [ps.stem(token) for token in tokenize(text)]
            word_frequency = compute_word_frequencies(tokens) #$$$$$$$$$$$$$$$$$$$$4
            
            for k, v in word_frequency.items():
                partial_index[k].append(Posting(i, len(v), v))
            
            i += 1
        
        self.write_partial_index(partial_index)
        
        with open(str(self.log_dir / "status.json"), "r") as file:
            status = json.load(file)
            status["read_batches"] += 1
            status["partial_index"] = 1
        
        with open(str(self.log_dir / "status.json"), "w") as file:
            json.dump(status, file)
        
        return partial_index
    
    '''
    Write the in-memory inverted index into disk
    use multi threading
    '''
    
    def write_batch(self, partial_index=None, recover=False):

        if partial_index is None:
            partial_index = {}
        
        if recover:
            partial_index = self.read_partial_index()
            with open(str(self.log_dir / "status.json"), "r") as file:
                written_terms = json.load(file)["written_terms"]
            
            written_terms = set(written_terms)
            print("Write from partial index")
            for item in partial_index.items():
                if item[0] in written_terms:
                    continue
                print(f"Writing {item[0]}")
                self.write_a_term(item[0], item[1])
        else:
            for item in partial_index.items():
                print(f"Writing {item[0]}")
                self.write_a_term(item[0], item[1])
  
        
        with open(str(self.log_dir / "status.json"), "r") as file:
            status = json.load(file)
            status["written_terms"] = []
            status["write_batches"] += 1
            status["partial_index"] = 0
        
        with open(str(self.log_dir / "status.json"), "w") as file:
            json.dump(status, file)
        
        os.remove(str(self.log_dir / "partial_index.txt"))
    
    # Functional helper function----------------------------------
    '''
        Helper function for multithreading
        write a term and its posting list at once
        '''
    
    def write_a_term(self, term: str, postings: list):
        first_char = term[0]
        old_postings = []
        try:
            with open(str(self.index_dir / first_char / term) + ".txt", "r",
                      encoding="utf-8") as file:
                old_postings = Posting.read_posting_list(file.read())
        except FileNotFoundError:
            pass
        
        postings.extend(old_postings)
        
        with open(str(self.index_dir / first_char / term) + ".txt", "w",
                  encoding="utf-8") as file:
            postings = sorted(postings, reverse=True)
            file.write(str(postings[0]))
            for posting in postings[1:]:
                file.write(";" + str(posting))
        
        Indexer.lock.acquire()
        with open(str(self.log_dir / "status.json"), "r") as file:
            status = json.load(file)
            status["written_terms"].append(term)
        with open(str(self.log_dir / "status.json"), "w") as file:
            json.dump(status, file)
        Indexer.lock.release()
    
    '''

    '''
    
    def caculate_tfidf_score(self):
        pass
    
    '''

    '''
    
    def read_index_file(self, term: str) -> list:
        pass
    
    def get_sim_hash(self, text: str):
        pass
    
    '''
    Write the dictionary partial_index into file partial_index.txt
    format:
    apple:[0, 3, [3, 4, 6]];[4, 3, [1, 2, 3]]
    beer:[5, 1, [9]];[7, 4, [5, 7, 34, 55]]
    '''
    
    def write_partial_index(self, partial_index: dict):
        with open(str(self.log_dir / "partial_index.txt"), "w") as file:
            for term, posting_list in partial_index.items():
                file.write(term + ":" + str(posting_list[0]))
                for posting in posting_list[1:]:
                    file.write(";" + str(posting))
                file.write("\n")
    
    '''
    Read partial_index.txt and return a dict of partial_index
    raise FileNotFoundError if partial_index.txt does not exist
    '''
    
    def read_partial_index(self) -> dict:
        try:
            with open(str(self.log_dir / "partial_index.txt"), "r") as file:
                datas = file.readlines()
                partial_index = {}
                for data in datas:
                    data = data.split(":")
                    partial_index[data[0]] = Posting.read_posting_list(data[1])
                return partial_index
        except FileNotFoundError:
            raise FileNotFoundError
    
    
    '''
    Read the src json files and construct docid_to_url.json and return the dict
    '''
    
    def get_docid_to_url(self, docid_to_path=None) -> dict:
        if docid_to_path is None:
            docid_to_path = list()
    
        try:
            with open(str(self.log_dir / "docid_to_url.json"), "r",
                      encoding="utf-8") as file:
                return json.load(file)
        
        except FileNotFoundError:
            if len(docid_to_path) == 0:
                raise FileNotFoundError
            
            i = 0
            docid_to_url = {}
            for p in docid_to_path:
                with open(p, "r", encoding="utf-8") as file:
                    url = json.load(file)["url"]
                docid_to_url[i] = url
                i += 1
            with open(str(self.log_dir / "docid_to_url.json"), "w",
                      encoding="utf-8") as file:
                json.dump(docid_to_url, file)
            
            return docid_to_url
    
    # ---------------------------------- Functional helper function
    
    # Environment initialization method------------------------------
    '''
    Create empty directory for index and log
    '''
    
    def create_dir(self):
        if not self.index_dir.exists():
            self.index_dir.mkdir()
        
        for char in list(string.ascii_lowercase):
            index_path = self.index_dir / char
            if not index_path.exists():
                index_path.mkdir()
        
        for num in list("0123456789"):
            index_path = self.index_dir / num
            if not index_path.exists():
                index_path.mkdir()
        
        if not self.log_dir.exists():
            self.log_dir.mkdir()

    '''
    Return a list of the absolute path of the input file
    Also construct json files:
    docid_path.json, map docid to absolutepath
    Store docid_path.json into log_dir
    '''
    
    def open_source_dir(self) -> list:
        docid_to_path = dict()
        try:
            with open(str(self.log_dir / "docid_to_path.json"), "r") as file:
                docid_to_path = json.load(file)
                return [docid_to_path[k] for k in sorted(docid_to_path.keys())]
        
        except FileNotFoundError:
            pass
        
        # create list of all subdirectories that we need to process
        directory_list = [dir for dir in self.src_dir.iterdir() if dir.is_dir()]
        
        docid = 0
        # getting all the .json file paths and adding them to a list to be processed by threads calling tokenize()
        # also creates a hashtable that maps docID => filepath urls
        for directory in directory_list:
            for file in Path(directory).iterdir():
                if file.is_file():
                    docid_to_path[int(docid)] = str(file.resolve())
                    docid += 1
        
        # Writes "docid_to_path" file that maps the docIDs to filepaths of
        # those documents.
        with open(str(self.log_dir / "docid_to_path.json"), "w") as file:
            json.dump(docid_to_path, file)
        
        return [docid_to_path[k] for k in sorted(docid_to_path.keys())]

    '''
        Create status.json if it doesn't exist
        for recording the process of indexing
        '''

    def get_status_json(self):
        try:
            with open(str(self.log_dir / "status.json"), "r") as file:
                return json.load(file)
    
        except FileNotFoundError:
            status = {"read_batches": 0, "write_batches": 0, "partial_index": 0,
                      "batch_size": self.batch_size, "written_terms": []}
            with open(str(self.log_dir / "status.json"), "w") as file:
                json.dump(status, file)
            return status
    
    # ------------------------------Environment initialization method


if __name__ == '__main__':
    path = Path("../Assginment3_data")
    if not path.exists():
        path.mkdir()
    srcPath = path / ".." / "DEV"
    destPath = path / "Index"
    logDir = path / "log"
    try:
        batch_size = sys.argv[1] #how many json file read and write at once
    except IndexError:
        batch_size = 10000
    
    indexer = Indexer(srcPath, destPath, logDir, int(batch_size))
    indexer.construct_index()

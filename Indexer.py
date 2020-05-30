

from pathlib import Path
import os
import sys
import re
import json
from bs4 import BeautifulSoup
import string
import math

from nltk.stem import PorterStemmer

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
    
    def __init__(self, src_dir: Path, index_dir: Path, log_dir: Path,
                 batch_size: int):
        self.src_dir = src_dir
        self.index_dir = index_dir
        self.log_dir = log_dir
        self.batch_size = batch_size
    
    def construct_index(self):
        self.create_dir()
        docid_to_path = self.open_source_dir()
        self.get_docid_to_url(docid_to_path)
        status = self.get_status_json()
        write_batch_count = status["write_batches"]
        while write_batch_count * self.batch_size < len(docid_to_path):
            status = self.get_status_json()
            read_batch_count = status["read_batches"]
            write_batch_count = status["write_batches"]
            
            if read_batch_count > write_batch_count:
                print(
                    f'Writing from partial index'
                    f' {(read_batch_count - 1) * self.batch_size} ~ '
                    f'{read_batch_count * self.batch_size}')
                self.write_batch(recover=True)
                print(f'Finish Indexing documents'
                      f' {(read_batch_count - 1) * self.batch_size} ~ '
                      f'{read_batch_count * self.batch_size}')
                continue
            
            print(
                f'Indexing documents {read_batch_count * self.batch_size} ~ '
                f'{(read_batch_count + 1) * self.batch_size}')
            
            partial_index = self.read_batch(docid_to_path, read_batch_count *
                                            self.batch_size, self.batch_size)
            print("partial index stored")
            self.write_batch(partial_index)
            
            print(f'Finish Indexing documents'
                  f' {read_batch_count * self.batch_size} ~ '
                  f'{(read_batch_count + 1) * self.batch_size}')
        
        print("--------------done !------------------")
    
    '''
    Read in certain number of source json file
    construct partial index
    return a partial inverted index dictionary, key is the term, value is the
    posting list.
    
    Also write partial_index into log_dir
    '''
    
    def read_batch(self, docid_to_path=None, start=0, limit=0) -> dict:
        if docid_to_path is None:
            docid_to_path = list()
        
        partial_index = defaultdict(lambda: list())
        ps = PorterStemmer()
        
        if (start + limit) < len(docid_to_path):
            end = start + limit
        else:
            end = len(docid_to_path)
        
        tag_list = {'title': 100, 'h1': 90, 'h2': 80, 'h3': 70, 'h4': \
            60, 'h5': 50, 'h6': 40, 'strong': 30,
                    'b': 20, 'a': 10, 'p': 1, 'span': 1, 'div': 1}
        
        for i in range(start, end):
            with open(docid_to_path[str(i)], "r") as file:
                json_data = json.load(file)
                content = json_data["content"]
            
            soup = BeautifulSoup(content, features="html.parser")
            
            word_frequency = defaultdict(lambda: int())
            
            # Different tags has different importance level
            for tag in tag_list:
                tag_content_list = soup.find_all(tag)
                for tag_content in tag_content_list:
                    text = tag_content.get_text()
                    tokens = [ps.stem(token) for token in tokenize(text)]
                    temp_frequency = compute_word_frequencies(tokens)
                    for item in temp_frequency.items():
                        if len(item[0]) > 30:
                            continue
                        word_frequency[item[0]] += tag_list[tag] * item[1]
            
            # tokens = [ps.stem(token) for token in tokenize(soup.get_text())]
            # word_frequency = compute_word_frequencies(tokens)
            
            # do similarity test here, uncompleted
            
            # ------------------------
            
            for k, v in word_frequency.items():
                partial_index[k].append(Posting(i, v))
        
        self.write_partial_index(partial_index)
        
        with open(str(self.log_dir / "status.json"), "r") as file:
            status = json.load(file)
            status["read_batches"] += 1
            status["partial_index"] += 1
        
        with open(str(self.log_dir / "status.json"), "w") as file:
            json.dump(status, file)
        
        return partial_index
    
    '''
    Write the in-memory inverted index into disk
    '''
    
    def write_batch(self, partial_index=None, recover=False):
        
        if partial_index is None:
            partial_index = {}
        
        if recover:
            partial_index = self.read_partial_index()
            with open(str(self.log_dir / "status.json"), "r") as file:
                written_terms = json.load(file)["written_terms"]
            
            print("Write from partial index")
            for item in partial_index.items():
                if written_terms > 0:
                    written_terms -= 1
                    continue
                # print(f"Writing {item[0]}")
                self.write_a_term(item[0], item[1])
        else:
            for item in partial_index.items():
                # print(f"Writing {item[0]}")
                self.write_a_term(item[0], item[1])
        
        with open(str(self.log_dir / "status.json"), "r") as file:
            status = json.load(file)
            status["written_terms"] = 0
            status["write_batches"] += 1
        
        with open(str(self.log_dir / "status.json"), "w") as file:
            json.dump(status, file)
    
    # Functional helper function----------------------------------
    '''
        Helper function for multithreading
        write a term and its posting list at once
        '''
    
    def write_a_term(self, term: str, postings: list):
        if len(term) < 100:
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
        
        with open(str(self.log_dir / "status.json"), "r") as file:
            status = json.load(file)
            status["written_terms"] += 1
        with open(str(self.log_dir / "status.json"), "w") as file:
            json.dump(status, file)
    
    '''

    '''
    
    def caculate_tfidf_score(self):
        print("Calculating tfidf score ...")
        N = 55393
        term_to_idf = defaultdict(lambda: float())
        
        for dir in self.index_dir.iterdir():
            if dir.is_dir():
                for file in dir.iterdir():
                    if file.is_file():
                        with open(str(file), "r") as f:
                            posting_list = Posting.read_posting_list(f.read())
                        
                        idf = math.log(float(N) / len(posting_list))
                        term = file.name[:-4]
                        term_to_idf[term] = idf
                        for posting in posting_list:
                            posting.tfidf = (1 + math.log(posting.tf)) * idf
                        
                        posting_list = sorted(posting_list, reverse=True)
                        
                        with open(str(file), "w") as f:
                            f.write(str(posting_list[0]))
                            for posting in posting_list[1:]:
                                f.write(";" + str(posting))
        
        with open(str(self.log_dir / "term_to_idf.json"), "w") as file:
            json.dump(term_to_idf, file)
        
        print("Calculating tfidf score done !")
    
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
        status = self.get_status_json()
        partial_index_num = status["partial_index"]
        
        with open(
            str(self.log_dir / f"partial_index_{partial_index_num + 1}.txt"),
            "w") as file:
            for key in sorted(partial_index.keys()):
                posting_list = partial_index[key]
                file.write(key + ":" + str(posting_list[0]))
                for posting in posting_list[1:]:
                    file.write(";" + str(posting))
                file.write("\n")
    
    '''
    Read partial_index.txt and return a dict of partial_index
    raise FileNotFoundError if partial_index.txt does not exist
    '''
    
    def read_partial_index(self) -> dict:
        status = self.get_status_json()
        partial_index_num = status["partial_index"]
        try:
            with open(
                str(self.log_dir / f"partial_index_{partial_index_num}.txt"),
                "r") as file:
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
            docid_to_path = dict()
        
        try:
            with open(str(self.log_dir / "docid_to_url.json"), "r",
                      encoding="utf-8") as file:
                return json.load(file)
        
        except FileNotFoundError:
            if len(docid_to_path) == 0:
                raise FileNotFoundError
            
            docid_to_url = {}
            for k, v in docid_to_path.items():
                with open(v, "r", encoding="utf-8") as file:
                    url = json.load(file)["url"]
                docid_to_url[k] = url
            
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
    
    def open_source_dir(self) -> dict:
        docid_to_path = dict()
        try:
            with open(str(self.log_dir / "docid_to_path.json"), "r") as file:
                docid_to_path = json.load(file)
                return docid_to_path
        
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
        
        return docid_to_path
    
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
                      "batch_size": self.batch_size, "written_terms": 0}
            with open(str(self.log_dir / "status.json"), "w") as file:
                json.dump(status, file)
            return status
    
    # ------------------------------Environment initialization method


if __name__ == '__main__':
    download = Path("/Users/ricardo/Downloads")
    
    # Where the source website json file store
    srcPath = download / "DEV"
    
    # Where you store the data
    data_path = download / "Assginment3_data"
    if not data_path.exists():
        data_path.mkdir()
    
    destPath = data_path / "Index"
    logDir = data_path / "log"
    try:
        batch_size = sys.argv[1]  # how many json file read and write at once
    except IndexError:
        batch_size = 15000
    
    indexer = Indexer(srcPath, destPath, logDir, int(batch_size))
    indexer.construct_index()
    indexer.caculate_tfidf_score()

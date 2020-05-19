import re
from collections import defaultdict
from bs4 import BeautifulSoup
import json

pattern = re.compile("[\W_]+")


def tokenize(text: str) -> list:
    tokens = []
    try:
        tokens = re.findall(r"[a-z0-9][a-z0-9]+", text.lower())
    except Exception as err:
        print(err)
    finally:
        return [token for token in tokens]


def compute_word_frequencies(tokens: list) -> dict:
    count = defaultdict(lambda: list())
    try:
        i = 0
        for token in tokens:
            count[token].append(i)
    except:
        pass
    finally:
        return count



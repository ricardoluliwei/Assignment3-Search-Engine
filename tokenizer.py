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

"""given a path"""
def tokenize_a_file(path: str):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
        content = data["content"]
        soup = BeautifulSoup(content, features="html.parser")
        text = soup.get_text()
        tokens = re.findall(r"[a-z0-9][a-z0-9]+", text)
    return tokens


def compute_word_frequencies(tokens: list) -> dict:
    count = defaultdict(int)
    try:
        for token in tokens:
            count[token] += 1
    except:
        pass
    finally:
        return count




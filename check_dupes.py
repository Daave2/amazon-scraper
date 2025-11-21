import json
import sys

def dict_raise_on_duplicates(ordered_pairs):
    d = {}
    for k, v in ordered_pairs:
        if k in d:
            print(f"Duplicate key found: {k}")
        d[k] = v
    return d

try:
    with open('config.example.json', 'r') as f:
        json.load(f, object_pairs_hook=dict_raise_on_duplicates)
    print("No duplicates found.")
except Exception as e:
    print(f"Error: {e}")

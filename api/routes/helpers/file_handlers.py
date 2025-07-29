from typing import List
import json
import os

def get_ticket_id_file_path(table_name: str) -> str:
    return f"{table_name}_ticket_ids.json"

def write_to_file(table_name: str, ticket_ids: List[str]) -> None:
    dir_name = "tmp"
    os.makedirs(dir_name, exist_ok=True)
    file_path = os.path.join(dir_name, get_ticket_id_file_path(table_name))
    try:
        with open(file_path, "w") as f:
            json.dump(ticket_ids, f)
    except Exception as e:
        print(f"Exception occurred while writing to file: {e}")
        raise

def read_from_file(table_name: str) -> List[str]:
    file_path = os.path.join("tmp", get_ticket_id_file_path(table_name))
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        os.remove(file_path)
        return data
    except FileNotFoundError:
        print("File not found.")
    except Exception as e:
        print(f"Exception occurred while reading file: {e}")
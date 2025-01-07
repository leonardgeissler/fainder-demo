import json

from fainder.utils import load_input

PATH_TO_INDEX = "../data/kaggle/indices/rebinning.zst"

PATH_TO_METADATA = "../data/kaggle/metadata"

INDEX = load_input(PATH_TO_INDEX, "index")

with open("../data/kaggle/metadata.json") as f:
    METADATA = json.load(f)

LIST_OF_HIST: list[str] = METADATA["list_of_hist"]

LIST_OF_DOCS: list[str] = METADATA["list_of_docs"]

DICT_FILE_ID_TO_HISTS: dict[str, list[int]] = METADATA["dict_file_id_to_hists"]

DICT_DOC_ID_TO_FILE_IDS: dict[str, list[int]] = METADATA["dict_doc_id_to_file_ids"]

import os
import shutil
import pandas as pd
from joblib import Parallel, delayed
from tqdm_joblib import tqdm_joblib

def sync(warc):
    if os.path.exists(f"{src_dir}/{warc}"):
        shutil.copyfile(f"{src_dir}/{warc}", f"{target_dir}/{warc}")
    else:
        print(f"missing file {src_dir}/{warc}")

if __name__ == "__main__":
    used_wars_data = pd.read_csv("data/android_howto/common_crawl_annotation.csv")
    warc_files = used_wars_data.warc_file.unique()
    used_wars_unique_rows = used_wars_data.drop_duplicates()
    warcs = [f.split('/')[-1] for f in used_wars_unique_rows.warc_file.unique()]
    src_dir = "data/android_howto/warc"
    target_dir = "data/android_howto/check"
    with tqdm_joblib(desc="Sync", total=len(warcs)) as progress_bar:
        Parallel(n_jobs=64)(delayed(sync)(url) for url in warcs)
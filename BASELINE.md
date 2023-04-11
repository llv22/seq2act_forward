# BASELINE reproduction

This document describes how to reproduce the results of the baseline model of seq2act

in /data/orlando/workspace/google-research/seq2act/data/android_howto/warc, there are only 94 data from CC-MAIN-20200328074047-20200328104047-00000.warc.gz to CC-MAIN-20200328074047-20200328104047-00099.warc.gz

CC-MAIN-20200328074047-20200328104047-00045.warc.gz
CC-MAIN-20200328074047-20200328104047-00047.warc.gz
CC-MAIN-20200328074047-20200328104047-00048.warc.gz
CC-MAIN-20200328074047-20200328104047-00049.warc.gz
CC-MAIN-20200328074047-20200328104047-00050.warc.gz
CC-MAIN-20200328074047-20200328104047-00051.warc.gz

```bash
cd /data/orlando/workspace/google-research
bash seq2act/data_generation/crawl_instructions.sh # crawled_instructions.json is then generated with each line as a Json string containing one instruction
wget https://raw.githubusercontent.com/google-research-datasets/seq2act/master/data/android_howto/common_crawl_annotation.csv 
mv common_crawl_annotation.csv seq2act/data/android_howto/
bash seq2act/data_generation/create_android_howto.sh # download Annotation File and Generate AndroidHowTo tfrecord

cd seq2act/data/rico_sca/raw
bash seq2act/data_generation/create_rico_sca.sh
```

## How to run the baseline

```bash
cd /data/orlando/workspace/google-research
tmux new 'python -m seq2act.data_generation.crawl_instructions --input_warc_dir=seq2act/data/android_howto/warc --output_instruction_json=seq2act/data/android_howto/crawled_instructions.json'
```
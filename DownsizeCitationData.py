import pandas as pd
from fastparquet import write
import numpy as np
import gc
import sys

'''Script for making http://opencitations.net csv dump (needs to be downloaded and extracted into a folder callled Data seperately)
 be in memory for 64 GBs (Could work for 32 and 16 GBs as well athough authorsc should be dropped)
 
 First part hashes the doi of the pairs of papers while keeping the information whether it is a selfcite as a bool
  and the second makes a backwards map between doi and hash (forward map is possible via hash(doi_string)
  
  REMEMEBER TO SET PYTHONHASHSEED=0 WHEN RUN OTHERWISE HASH SEED IS RANDOM
  
  Hash map should be fine up to  hundreds of billions of papers, currently has 60 million papers, thus false positives
  baiscally non-existent
 '''

def main():

    names = []
    for i in range(1, 64):
        names.append('Data/2019-10-21T22:41:20_' + str(i) + '.csv')
    for i in range(1, 5):
        names.append('Data/2020-01-13T19:31:19_' + str(i) + '.csv')
    for i in range(1, 6):
        names.append('Data/2020-04-25T04:48:36_' + str(i) + '.csv')
    for i in range(1, 3):
        names.append('Data/2020-06-13T18:18:05_' + str(i) + '.csv')
    for i in range(1, 3):
        names.append('Data/2020-08-20T18:12:28_' + str(i) + '.csv')
    for i in range(1, 4):
        names.append('Data/2020-11-22T17:48:01_' + str(i) + '.csv')
    if sys.argv[1]:
        split = int(sys.argv[1])
    else:
        split = 4
    for s in range(0, split):

        old_dataframe = pd.read_csv(names[int(s * len(names) / split)], sep=',', dtype=str, low_memory=False)
        old_dataframe = old_dataframe.drop(['oci', 'creation', 'timespan', 'journal_sc'], axis=1)
        old_dataframe['author_sc'] = pd.Series(np.where(old_dataframe.author_sc == 'yes', 1, 0))
        hs = old_dataframe['citing'].values
        new_hash = np.zeros(len(hs), dtype='int64')
        for j in range(0, len(hs)):
            new_hash[j] = hash(hs[j])
        old_dataframe['citing'] = new_hash
        hs = old_dataframe['cited'].values
        new_hash = np.zeros(len(hs), dtype='int64')
        for j in range(0, len(hs)):
            new_hash[j] = hash(hs[j])
        old_dataframe['cited'] = new_hash
        old_dataframe = old_dataframe

        for i in range(int(len(names) * s / split + 1), int(len(names) * (s + 1) / split)):
            dataframe = pd.read_csv(names[i], sep=',', dtype=str, low_memory=False)
            dataframe = dataframe.drop(['oci', 'creation', 'timespan', 'journal_sc'], axis=1)
            dataframe['author_sc'] = pd.Series(np.where(dataframe.author_sc == 'yes', 1, 0))
            hs = dataframe['citing'].values
            new_hash = np.zeros(len(hs), dtype='int64')
            for j in range(0, len(hs)):
                new_hash[j] = hash(hs[j])
            dataframe['citing'] = new_hash
            hs = dataframe['cited'].values
            new_hash = np.zeros(len(hs), dtype='int64')
            for j in range(0, len(hs)):
                new_hash[j] = hash(hs[j])
            dataframe['cited'] = new_hash
            old_dataframe = pd.concat([old_dataframe, dataframe])
            del dataframe
            gc.collect()

        write('Name' + str(s) + '.parq', old_dataframe)
        del old_dataframe
        gc.collect()

    old_dataframe = pd.read_parquet('Name0.parq', engine='fastparquet')
    for i in range(1, split):
        dataframe = pd.read_parquet('Name'+str(i)+'.parq', engine='fastparquet')
        old_dataframe = pd.concat([old_dataframe, dataframe])
    write('AllCitations.parq', old_dataframe)


    unique_dois = np.array([], dtype=object)
    for i in range(0, len(names)):
        dataframe = pd.read_csv(names[i], sep=',', dtype=str, low_memory=False)
        unique_dois = np.concatenate((unique_dois, dataframe['citing'].unique(), dataframe['cited'].unique()), axis=None)
        unique_dois = np.unique(unique_dois)
        del dataframe
        gc.collect()
    hs = np.empty(len(unique_dois))
    for j in range(0, len(hs)):
        hs[j] = hash(unique_dois[j])

    d = {'doi': unique, 'hash': hs}
    dfg = pd.DataFrame(data=d)

    write('HashedDoiMap.parq', dfg)


if __name__ == "__main__":
    main()

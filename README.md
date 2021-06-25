# opencitations_Cleanup
Script for making http://opencitations.net csv data dump (needs to be downloaded and extracted into a folder callled Data seperately)
 be in memory for ram with 64 GBs (Could work for 32 and 16 GBs as well athough authorsc should be dropped). For future releases just add the names of the files that have been added to names
 
To run download dataset (roughly 120Gbs of space), extract into Data then run

PYTHONHASHSEED=0 python3 DownsizeCitationData.py
  
This produces AllCitations.parq and HashedDoiMap.parq which contain the hashed pairs of refrences and hash to doi map respectively, total size 18 Gbs. For small Rams run

PYTHONHASHSEED=0 python3 DownsizeCitationData.py n

where the n at the end is as much as you think your ram can manage at the time (ie. 32/n)

 
REMEMEBER TO SET PYTHONHASHSEED=0 WHEN RUN OTHERWISE HASH SEED IS RANDOM and the result will be garbage. In jupyter or pycharm this can easily be set for the whole interpreter.
  
Hash map should be fine up to  hundreds of billions of papers, currently has 60 million papers, thus false positives are guaranteed to be non-existent

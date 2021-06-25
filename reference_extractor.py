import pandas
import numpy
import fastparquet
from fastparquet import ParquetFile
import sys
import gc

sys.path.append('..')

'''
Class to hold hashed dois of citing papers and cited papers and to provide methods  
'''


class ReferenceExtractor:

    def __init__(self):
        pf = ParquetFile('../AllCitations.parq')
        self.all_citations = pf.to_pandas()
        pf = ParquetFile('../HashedDoiMap.parq')
        self.hash_map = pf.to_pandas()

    def cited_by_string(self, doi):
        """

        :param doi: the doi of the document as a string that was cited by other documents
        :return: the hashes of the dois other documents
        """
        return self.all_citations['citing'].loc[self.all_citations['cited'] == hash(doi)].values

    def cited_by_hash(self, doi_hash):
        """

        :param doi: the doi of the document as a hash that was cited by other documents
        :return: the hashes of the dois other documents
        """
        return self.all_citations['citing'].loc[self.all_citations['cited'] == doi_hash].values

    def citing_hash(self, doi_hash):
        """

        :param doi: the doi of the document that cites other documents as a hash
        :return: the hashes of the dois other documents
        """
        return self.all_citations['cited'].loc[self.all_citations['citing'] == doi_hash].values

    def citing_string(self, doi):
        """

        :param doi: the doi of the document that cites other documents as a hash
        :return: the hashes of the dois other documents
        """
        return self.all_citations['cited'].loc[self.all_citations['citing'] == hash(doi)].values

    def paper_doi_from_hash(self, doi_hash):
        '''

        :param doi_hash: the hash of the  doi of the document
        :return: the doi as a string
        '''
        return self.hash_map['doi'].loc[self.hash_map['hash'] == doi_hash].values[0]

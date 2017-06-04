from pathlib2 import Path
import logging
from datetime import datetime
import pandas as pd
from Bio import Entrez
import time

Entrez.email = "@gmail.com"

data_in_catalog = "InfoIn"
data_out_catalog = 'InfoOut'

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

startTime = datetime.now()

data_in = Path('.') / data_in_catalog
bacteria_paths = [x for x in data_in.iterdir() if x.is_file()]


def record_to_pubmed_id(r):
    if 'GBSeq_references' not in r:
        return None
    references = r['GBSeq_references'][0]
    if len(r['GBSeq_references']) == 0:
        return None
    if 'GBReference_pubmed' not in references:
        return None
    return references.get('GBReference_pubmed')


def record_to_title_first(r):
    if 'GBSeq_references' not in r:
        return None
    if len(r['GBSeq_references']) == 0:
        return None
    references = r['GBSeq_references'][0]
    if 'GBReference_title' not in references:
        return None
    return references.get('GBReference_title')


def record_to_title_second(r):
    if 'GBSeq_references' not in r:
        return None
    if len(r['GBSeq_references']) <= 1:
        return None
    references = r['GBSeq_references'][1]
    if 'GBReference_title' not in references:
        return None
    return references.get('GBReference_title')


def record_to_journal_first(r):
    if 'GBSeq_references' not in r:
        return None
    if len(r['GBSeq_references']) == 0:
        return None
    references = r['GBSeq_references'][0]
    if 'GBReference_journal' not in references:
        return None
    return references.get('GBReference_journal')


def record_to_journal_second(r):
    if 'GBSeq_references' not in r:
        return None
    if len(r['GBSeq_references']) <= 1:
        return None
    references = r['GBSeq_references'][1]
    if 'GBReference_journal' not in references:
        return None
    return references.get('GBReference_journal')


def record_to_authors_first(r):
    if 'GBSeq_references' not in r:
        return None
    if len(r['GBSeq_references']) == 0:
        return None
    references = r['GBSeq_references'][0]
    if 'GBReference_authors' not in references:
        return None
    return ";".join(references["GBReference_authors"])


def record_to_authors_second(r):
    if 'GBSeq_references' not in r:
        return None
    if len(r['GBSeq_references']) <= 1:
        return None
    references = r['GBSeq_references'][1]
    if 'GBReference_authors' not in references:
        return None
    return ";".join(references["GBReference_authors"])


def record_to_primary_accession(r):
    if "GBSeq_primary-accession" not in r:
        return None
    return r["GBSeq_primary-accession"]


def record_to_definition(r):
    if "GBSeq_definition" not in r:
        return None
    return r["GBSeq_definition"]


def record_to_isolation_source(r):
    if "GBSeq_feature-table" not in r:
        return None
    qualifiers = r['GBSeq_feature-table'][0]['GBFeature_quals']
    for qualifier in qualifiers:
        if qualifier['GBQualifier_name'] == 'isolation_source':
            return qualifier['GBQualifier_value']


def record_to_country(r):
    if "GBSeq_feature-table" not in r:
        return None
    qualifiers = r['GBSeq_feature-table'][0]['GBFeature_quals']
    for qualifier in qualifiers:
        if qualifier['GBQualifier_name'] == 'country':
            return qualifier['GBQualifier_value']


def record_to_collection_date(r):
    if "GBSeq_feature-table" not in r:
        return None
    qualifiers = r['GBSeq_feature-table'][0]['GBFeature_quals']
    for qualifier in qualifiers:
        if qualifier['GBQualifier_name'] == 'collection_date':
            return qualifier['GBQualifier_value']


for microbe_path in bacteria_paths:
    df_out = pd.DataFrame()
    logging.info("Processing: {0}".format(microbe_path))

    df = pd.read_csv(str(microbe_path))
    for index, seq in df.iterrows():
        logging.info("\t{0}".format(seq['Sequence Name']))
        handle = Entrez.efetch(db="nucleotide", id=seq['Sequence Name'], retmode="xml")
        record = Entrez.read(handle)
        handle.close()

        df_out = df_out.append({'pubmed': record_to_pubmed_id(record[0]),
                                'title_1': record_to_title_first(record[0]),
                                'title_2': record_to_title_second(record[0]),
                                'journal_first': record_to_journal_first(record[0]),
                                'journal_second': record_to_journal_second(record[0]),
                                'authors_first': record_to_authors_first(record[0]),
                                'authors_second': record_to_authors_second(record[0]),
                                'primary_accession': record_to_primary_accession(record[0]),
                                'definition': record_to_definition(record[0]),
                                'isolation_source': record_to_isolation_source(record[0]),
                                'country': record_to_country(record[0]),
                                'collection_date': record_to_collection_date(record[0])
                                },
                               ignore_index=True)

        time.sleep(3)
    # print df_out
    microbe = microbe_path.stem

    p = Path('.')
    path = p / data_out_catalog / microbe

    path.mkdir(parents=True, exist_ok=True)
    "{0}/{1}.html".format(path, microbe)
    writer = pd.ExcelWriter('{0}/{1}.xlsx'.format(path, microbe))

    result = pd.concat([df, df_out], axis=1, join='inner')
    result.to_excel(writer, 'All_results')
    # df_out.to_excel(writer, 'out')
    # df.to_excel(writer, 'in')
    writer.save()
logging.info("Ended. Done in {0}".format(datetime.now() - startTime))



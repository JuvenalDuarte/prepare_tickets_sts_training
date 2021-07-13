from pycarol import Carol, Staging
import logging
import pandas as pd
import numpy as np
import re
import json
import re
from unidecode import unidecode
from bs4 import BeautifulSoup
import ftfy

logger = logging.getLogger(__name__)

def fetchFromCarol(env, conn, stag, columns=None):
    carol = Carol()
    carol.switch_environment(env_name=env, app_name='zendeskdata')

    try:
        df = Staging(carol).fetch_parquet(staging_name=stag, connector_name=conn, backend='pandas', columns=columns, cds=True)

    except Exception as e:
        logger.error(f'Failed to fetch dada. {e}')
        df =  pd.DataFrame()

    return df

def remove_html_tags(text):
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    return " ".join(re.sub(r'\s([?.!"](?:\s|$))', r'\1', text).strip().split())

def get_question(body):
    body = str(body)
    body = re.sub('º|ª|°|˚|\u200b', '', body)
    body = re.sub('\xa0', ' ', body)      
    body = re.sub('\n', '<br>', body)  
    body = repr(body)

    m = re.search('(?<=D(ú|u)vida).*?(?=Ambiente)', body)
    if m:
        return re.sub('([a-zA-Z])-(\d+)', '\\1\\2', remove_html_tags(m.group(0)))
        
    m = re.search('(?<=Ocorr(ê|e)ncia).*?(?=Ambiente)', body)
    if m:
        return re.sub('([a-zA-Z])-(\d+)', '\\1\\2', remove_html_tags(m.group(0)))

    m = re.search('(?<=D(ú|u)vida).*?(?=Solução)', body)
    if m:
        return re.sub('([a-zA-Z])-(\d+)', '\\1\\2', remove_html_tags(m.group(0)))

    m = re.search('(?<=Ocorr(ê|e)ncia).*?(?=Solução)', body)
    if m:
        return re.sub('([a-zA-Z])-(\d+)', '\\1\\2', remove_html_tags(m.group(0)))
    
    return np.nan

def get_question_type(body):
    body = str(body)
    body = re.sub('º|ª|°|˚|\u200b', '', body)
    body = re.sub('\xa0', ' ', body)      
    body = re.sub('\n', '<br>', body)   
    body = repr(body)

    #m = re.search('(?<=<strong>D(ú|u)vida).*?(?=<strong>Ambiente)', body)
    m = re.search('(?<=D(ú|u)vida).*?(?=Ambiente)', body)
    if m:
        return 'question'

    m = re.search('(?<=Ocorr(ê|e)ncia).*?(?=Ambiente)', body)
    if m:
        return 'occurrence'

    m = re.search('(?<=D(ú|u)vida).*?(?=Solução)', body)
    if m:
        return 'question'

    m = re.search('(?<=Ocorr(ê|e)ncia).*?(?=Solução)', body)
    if m:
        return 'occurrence'
    
    return np.nan

def parse_satisfaction_score(txt):
    try:
        d = json.load(txt)
        score = d["score"]
    except:
        score = "bad"

    return score

def preproc(txt):
    # Ensure the parameter type as string
    mproc0 = str(txt)
    
    # Ensure the parameter type as string
    mproc1 = BeautifulSoup(mproc0).text
    
    # Set all messages to a standard encoding
    mproc2 = ftfy.fix_encoding(mproc1)
    
    # Replaces accentuation from chars. Ex.: "férias" becomes "ferias" 
    mproc3 = unidecode(mproc2)

    return mproc3


def mapScoreTosimilarity(score):
    if score in ["good", "offered"]:
        return 1
    else:
        return 0

def ingest_tickets():
    environment, connector_name, stagging = ("datalake", "Zendesk", "tickets_articles_sts_training")
    logger.info(f'Retrieving data from {environment}/{connector_name}/{stagging}.')
    tickets_articles = fetchFromCarol(env=environment, conn=connector_name, stag=stagging)

    logger.info(f'Parsing question from article body.')
    tickets_articles["question"] = tickets_articles["body"].apply(get_question)
    tickets_articles["question_type"] =  tickets_articles["body"].apply(get_question_type)

    logger.info(f'Parsing customer satisfaction.')
    tickets_articles["satisfactionScore"] = tickets_articles["satisfaction_rating"].apply(parse_satisfaction_score)

    logger.info(f'Parsing customer satisfaction.')
    tickets_articles["ticket_subject"] = tickets_articles["subject"].apply(preproc)
    tickets_articles["article_title"] = tickets_articles["title"].apply(preproc)
    tickets_articles["article_question"] = tickets_articles["question"].apply(preproc)
    tickets_articles["similarity"] = tickets_articles["satisfactionScore"].apply(mapScoreTosimilarity)

    trainingset1 = tickets_articles[["ticket_subject", "article_title", "similarity"]].copy()
    trainingset2 = tickets_articles[["ticket_subject", "article_question", "similarity"]].copy()

    return trainingset1, trainingset2

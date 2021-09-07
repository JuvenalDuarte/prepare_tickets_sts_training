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

def fetchFromCarol(org, env, conn, stag, columns=None):
    carol = Carol()
    carol.switch_environment(org_name=org, env_name=env, app_name='zendeskdata')

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
        d = json.loads(txt)
        score = d["score"]
    except:
        score = "unparsed"

    return score

def preproc_basic(txt):
    # Ensure the parameter type as string
    mproc0 = str(txt)
    
    # Ensure the parameter type as string
    mproc1 = BeautifulSoup(mproc0).text
    
    # Set all messages to a standard encoding
    mproc2 = ftfy.fix_encoding(mproc1)
    
    # Replaces accentuation from chars. Ex.: "férias" becomes "ferias" 
    mproc3 = unidecode(mproc2)

    return mproc3

def preproc_advanced(m):
    global custom_stopwords

    # Ensure the parameter type as string
    # Ensure the parameter type as string
    # Set all messages to a standard encoding
    # Replaces accentuation from chars. Ex.: "férias" becomes "ferias" 
    mproc3 = preproc_basic(m)
    
    # Removes special chars from the sentence. Ex.: 
    #  - before: "MP - SIGAEST - MATA330/MATA331 - HELP CTGNOCAD"
    #  - after:  "MP   SIGAEST   MATA330 MATA331   HELP CTGNOCAD"
    mproc4 = re.sub('[^0-9a-zA-Z]', " ", mproc3)

    return mproc4

def preproc_stopwords(m):
    mproc4 = preproc_advanced(m)

    # Sets capital to lower case maintaining full upper case tokens and remove portuguese stop words.
    #  - before: "MP   MEU RH   Horario ou Data registrado errado em solicitacoes do MEU RH"
    #  - after:  "MP MEU RH horario data registrado errado solicitacoes MEU RH"
    mproc5 = " ".join([t.lower() for t in mproc4.split() if t not in custom_stopwords])

    return mproc5


def mapScoreTosimilarity(score):
    if score in ["good", "offered"]:
        return 1
    else:
        return 0

def random_undersampling(df):
    pos_samples = df[df["similarity"] == 1].copy()
    neg_samples = df[df["similarity"] == 0].copy()

    if len(pos_samples) > len(neg_samples):
        pos_samples = pos_samples.sample(len(neg_samples))
    else:
        neg_samples = neg_samples.sample(len(pos_samples))

    return pd.concat([pos_samples, neg_samples], ignore_index=True)

def filterTrash(ticket_subject):
    if (len(ticket_subject) < 10) or ("chat with" in ticket_subject) or (ticket_subject in ["nan", "atendimento telefonema"]):
        return False
    else:
        return True

def ingest_tickets(preproc_mode, undersampling, sats_filter):
    global custom_stopwords

    logger.info(f'Reading list of custom stopwords.')
    # Reading stopwords  to be removed
    with open('/app/cfg/stopwords.txt') as f:
        custom_stopwords = f.read().splitlines()

    organization, environment, connector_name, stagging = ("totvs", "datalake", "Zendesk", "tickets_articles_sts_training")
    logger.info(f'Retrieving data from {environment}/{connector_name}/{stagging}.')
    tickets_articles = fetchFromCarol(org=organization, env=environment, conn=connector_name, stag=stagging)

    organization, environment, connector_name, stagging = ("totvs", "protheusassistant", "catalogototvs", "catalogototvs")
    logger.info(f'Retrieving data from {environment}/{connector_name}/{stagging}.')
    totvs_catalogue = fetchFromCarol(org=organization, env=environment, conn=connector_name, stag=stagging)

    logger.info(f'Merging pairs to the catalogue to retrieve product, module and segment.')
    tickets_articles = pd.merge(tickets_articles, totvs_catalogue, on="section_id", how="inner", validate="m:1")

    logger.info(f'Parsing question from article body.')
    tickets_articles["question"] = tickets_articles["body"].apply(get_question)
    tickets_articles["question_type"] =  tickets_articles["body"].apply(get_question_type)

    logger.info(f'Parsing customer satisfaction.')
    tickets_articles["satisfactionScore"] = tickets_articles["satisfaction_rating"].apply(parse_satisfaction_score)

    summary = tickets_articles.groupby(["satisfactionScore"])["ticket_id"].count().to_string()
    logger.info(f'Satisfaction count summary: {summary}')

    if preproc_mode == "advanced":
        preproc = preproc_advanced

    elif preproc_mode == "stopwords":
        preproc = preproc_stopwords
        
    else:
        preproc = preproc_basic

    logger.info(f'Applying preprocessing to tickets subject.')
    tickets_articles["ticket_subject"] = tickets_articles["subject"].apply(preproc)

    logger.info(f'Applying preprocessing to articles title.')
    tickets_articles["article_title"] = tickets_articles["title"].apply(preproc)

    logger.info(f'Applying preprocessing to articles question.')
    tickets_articles["article_question"] = tickets_articles["question"].apply(preproc)

    if sats_filter:
        sats_list = [ i.lstrip().rstrip() for i in sats_filter.split(",")]
        logger.info(f'Filtering only tickets with satisfaction score in: {sats_list}.')

        tickets_articles = tickets_articles[tickets_articles["satisfactionScore"].isin(sats_list)].copy()

    logger.info(f'Mapping satisfaction to expected similarity.')
    tickets_articles["similarity"] = tickets_articles["satisfactionScore"].apply(mapScoreTosimilarity)

    if undersampling:
        logger.info(f'Balancing dataset through under sampling on the majority class.')
        tickets_articles = random_undersampling(tickets_articles)

    trainingset1 = tickets_articles[["ticket_subject", "article_title", "similarity", "article_id", "module", "product", "segment"]].copy()
    trainingset2 = tickets_articles[["ticket_subject", "article_question", "similarity", "article_id", "module", "product", "segment"]].copy()

    logger.info(f'Filtering bad samples.')
    trainingset2 = trainingset2[trainingset2["article_question"] != "nan"]
    trainingset2 = trainingset2[trainingset2["ticket_subject"].apply(filterTrash)]
    trainingset1 = trainingset1[trainingset1["ticket_subject"].apply(filterTrash)]

    trainingset3 = trainingset1.copy()
    trainingset3.rename(columns={"article_title":"article"}, inplace=True)
    trainingset4 = trainingset2.copy()
    trainingset4.rename(columns={"article_question":"article"}, inplace=True)
    trainingset3 = pd.concat([trainingset3, trainingset4], ignore_index=True)

    return trainingset1, trainingset2, trainingset3

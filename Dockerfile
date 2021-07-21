FROM totvslabs/pycarol:2.40.0

RUN mkdir /app
RUN mkdir /app/cfg
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt

# Download the list of stopwords from github repository
RUN wget https://raw.githubusercontent.com/JuvenalDuarte/portuguese_stopwords/main/stopwords.txt --no-verbose -P /app/cfg

ADD . /app

RUN rm -rf tmp

CMD ["python", "run.py"]

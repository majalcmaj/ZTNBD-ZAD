FROM jupyter/pyspark-notebook

RUN pip install textblob nltk
RUN python -m nltk.downloader punkt averaged_perceptron_tagger

ENTRYPOINT jupyter lab

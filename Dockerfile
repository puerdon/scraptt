FROM python:3.6.2
ENV PYTHONBUFFERED 1
RUN mkdir /scraptt
ADD . /scraptt
RUN pip install -r /scraptt/requirements.txt
WORKDIR /scraptt
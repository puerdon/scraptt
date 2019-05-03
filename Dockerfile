FROM python:3.7.3-alpine3.9
RUN apk add --update tzdata gcc libffi-dev openssl-dev musl-dev libxml2-dev libxslt-dev
ENV TZ=Asia/Taipei
RUN mkdir /scraptt
ADD requirements.txt /scraptt
RUN pip install -r /scraptt/requirements.txt
WORKDIR /scraptt

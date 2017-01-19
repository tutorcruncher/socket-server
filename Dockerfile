FROM python:3.6-alpine
RUN apk --update --no-cache add postgresql-dev gcc musl-dev jpeg-dev zlib-dev \
 && rm -rf /var/cache/apk/*
WORKDIR /home/root
ADD ./requirements.txt /home/root/requirements.txt
RUN pip install -r requirements.txt
ADD ./app /home/root/app
ADD ./settings.yml /home/root/settings.yml
ADD ./run.py /home/root/run.py
ENTRYPOINT ["./run.py"]
CMD ["web"]

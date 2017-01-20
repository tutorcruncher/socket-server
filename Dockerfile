FROM python:3.6-alpine

RUN apk --update --no-cache add postgresql-dev gcc g++ musl-dev jpeg-dev zlib-dev libuv make \
 && rm -rf /var/cache/apk/*

ADD ./requirements.txt /home/root/requirements.txt
RUN pip install -r /home/root/requirements.txt

ADD ./app /home/root/app
ADD ./settings.yml /home/root/settings.yml
ADD ./run.py /home/root/run.py

WORKDIR /home/root
HEALTHCHECK --interval=20s --timeout=10s --retries=2 CMD /home/root/run.py check || exit 1
ENTRYPOINT ["./run.py"]
CMD ["web"]

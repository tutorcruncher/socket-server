FROM python:3.9.21-alpine

RUN apk --update --no-cache add gcc g++ musl-dev zlib-dev libuv libffi-dev make postgresql-dev jpeg-dev tiff-dev \
 && rm -rf /var/cache/apk/*

ADD ./requirements.txt /home/root/requirements.txt
RUN pip install -r /home/root/requirements.txt

ADD ./app /home/root/app
ADD ./run.py /home/root/run.py

ENV PYTHONUNBUFFERED 1
WORKDIR /home/root
ENTRYPOINT ["python ./run.py"]
CMD ["auto"]

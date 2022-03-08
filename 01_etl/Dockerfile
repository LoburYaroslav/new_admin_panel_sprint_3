FROM python:3.9.5-alpine

WORKDIR /app

RUN  \
    apk update && \
    apk upgrade && \
    pip install --upgrade pip

COPY ./requirements.txt .

# install dependencies with additional dependencies for psycopg2
RUN   \
     apk add --no-cache postgresql-libs &&  \
     apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
     python3 -m pip install -r /app/requirements.txt --no-cache-dir &&  \
     apk --purge del .build-deps


# copy project
COPY ./app /app

CMD python3 main.py
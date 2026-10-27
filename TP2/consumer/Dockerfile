# Consommateur HealthStream (seance 2).
# Base Chainguard imposee par l'enonce ; le digest est fige par s2.sh au premier build.
FROM cgr.dev/chainguard/wolfi-base:latest@sha256:0b179206ab162b12a43bacea1b525a4023e0272005c92d08555c61b1beaedc40

RUN apk add --no-cache python-3.12

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Les dependances d'abord (changent rarement), le code ensuite (change souvent).
# pip n'est present que le temps de ce RUN : roues precompilees, rien ne l'appelle a l'execution.
COPY requirements.txt ./
RUN apk add --no-cache py3.12-pip \
    && python3.12 -m pip install --no-cache-dir --break-system-packages -r requirements.txt \
    && apk del --no-cache py3.12-pip

COPY lecteur.py consumer.py ./

# Meme UID qu'en seance 1 : coherent sur les volumes.
# chmod : les fichiers copies depuis le Mac peuvent etre en 600 (lisibles par leur seul
# proprietaire) ; appuser doit pouvoir lire le code.
RUN chmod -R a+rX /app && adduser -D -u 10001 appuser
USER appuser

CMD ["python3.12", "consumer.py"]

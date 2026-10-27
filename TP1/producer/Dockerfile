# 1. Image de base légère, version figée (jamais latest)
FROM python:3.12-slim

# 2. Pas de .pyc ; sortie non bufferisée (sinon docker logs reste muet)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Répertoire applicatif
WORKDIR /app

# 4-5. Dépendances d'abord : couche qui change rarement, donc mise en cache
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 6. Code ensuite : il change souvent, seule cette couche est reconstruite
COPY producer.py ./

# 7. Compte non privilégié UID 10001 ; /app/logs créé pour le volume nommé
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/logs \
    && chown -R appuser:appuser /app

# 8. Bascule définitive avant CMD
USER appuser

# 9. Commande par défaut
CMD ["python", "producer.py"]

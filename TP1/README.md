# TP1 — Conteneurs et flux (HealthStream)

Séance 1 du module *Infrastructure et architecture de données* : un broker **Kafka en mode KRaft** sous Docker Compose et un **producteur Python** qui publie les signes vitaux de 12 patients simulés sur le topic `mimic-vitals`.

## Contenu

| Chemin | Rôle |
|---|---|
| `docker-compose.yml` | Services `kafka` (KRaft, listeners interne/hôte) et `producer` (`depends_on` + `service_healthy`), volumes nommés, réseau dédié |
| `producer/Dockerfile` | Image `python:3.12-slim`, ordre optimisé pour le cache, utilisateur non-root (UID 10001) |
| `producer/producer.py` | Producteur `confluent-kafka` : clé `patient_id`, `acks=all`, idempotence, partitionneur `murmur2_random` |
| `MESURES.md` | Les 8 mesures, les 8 réponses aux questions et les décisions retenues |
| `Enonce_TP1.pdf` | Énoncé du TP |

## Lancer

```bash
docker compose up -d kafka
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic mimic-vitals --partitions 3 --replication-factor 1
docker compose up -d --build
docker compose logs -f producer
```

Arrêt : `docker compose down` (conserve les données) ou `docker compose down -v` (supprime aussi les volumes).

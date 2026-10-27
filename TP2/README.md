# TP2 — Ingestion et persistance (HealthStream)

Séance 2 du module *Infrastructure et architecture de données* : un **consommateur Kafka** lit le topic `mimic-vitals` et écrit les signes vitaux dans **PostgreSQL**, sans perte ni doublon, y compris quand la base tombe ou qu'un consommateur est tué.

```
producteur → Kafka (mimic-vitals, 6 partitions) → consommateur (groupe vitals-persister) → PostgreSQL (vitals_raw)
```

## Contenu

| Chemin | Rôle |
|---|---|
| `docker-compose.yml` | 4 services : `kafka`, `postgres` (Chainguard, healthcheck `pg_isready`), `producer`, `consumer` (`depends_on` sur les deux services à état, `restart: unless-stopped`, sans `container_name` pour pouvoir le dupliquer) |
| `sql/init.sql` | Table `vitals_raw`, clé primaire `pk_vitals_raw (kafka_partition, kafka_offset)` qui porte l'idempotence, contrainte `ck_vital_type`, index `idx_vitals_patient_time`, vue `v_vitals_controle` |
| `consumer/consumer.py` | Consommateur de persistance : lots de 200 ou 5 s, `INSERT … ON CONFLICT DO NOTHING`, acquittement manuel **après** l'écriture, reconnexion à PostgreSQL, stratégie `cooperative-sticky` |
| `consumer/lecteur.py` | Lecteur exploratoire (offsets, acquittement, rééquilibrage) |
| `consumer/Dockerfile` | Base `wolfi-base` figée par digest, pip retiré après installation, utilisateur non-root (UID 10001) |
| `MESURES_S2.md` | Les 19 mesures et les 19 réponses aux questions |
| `captures_s2/` | Sorties brutes de chaque partie |
| `s2.sh`, `outils/repartition.py` | Script qui rejoue chaque partie et relève les mesures |
| `S2_KORICH_Marwan.zip` | Archive rendue |

## Résultats clés

- Rééquilibrage : 3,9 s après un arrêt propre, 46 s après un `kill -9` (session de 45 s).
- PostgreSQL coupé 90 s : aucun offset acquitté pendant la coupure, retard rattrapé seul au retour.
- `SIGKILL` du consommateur : 0 doublon, 0 trou d'offset sur 411 661 lignes.

## Lancer

```bash
docker compose up -d --build
docker compose exec postgres psql -U hs_user -d healthstream -c 'SELECT * FROM v_vitals_controle;'
```

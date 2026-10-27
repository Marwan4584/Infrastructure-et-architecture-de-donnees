# Infrastructure et architecture de données

Master 2 IA & Data, Ynov Campus Montpellier, 2026-2027.
Fil rouge : **HealthStream ICU Monitor**, la chaîne de données des moniteurs d'un service de réanimation de 12 lits.

| TP | Séance | Contenu |
|---|---|---|
| [`TP1/`](TP1/) | Séance 1 — Conteneurs et flux | Broker Kafka en mode KRaft sous Docker Compose, producteur Python des signes vitaux (topic `mimic-vitals`) |
| [`TP2/`](TP2/) | Séance 2 — Ingestion et persistance | Consommateur Kafka, consumer groups et rééquilibrage, PostgreSQL, insertion par lots idempotente, lag et reprise après panne sans perte ni doublon |

Chaque dossier contient son `README.md`, son code et son fichier de mesures.

# MESURES — Séance 1 : Conteneurs et flux

## Versions
| Outil | Version |
|---|---|
| Docker Engine | 29.4.3 |
| Docker Compose | v5.1.4 |
| Python | 3.11.5 |

---

## Partie 1 — Docker

**Q1.1** — Non, c2 ne voit pas le fichier écrit par c1 (« fichier absent »). Chaque conteneur possède sa propre couche inscriptible au-dessus des couches en lecture seule de l'image : ce qu'écrit c1 reste dans sa couche et disparaît avec lui.

### Mesure 1
| Grandeur | Valeur relevée |
|---|---|
| Taille de python:3.12-slim | 145 MB |
| Taille de l'image finale (vitals-producer:0.1) | 193 MB |
| Nombre de couches non vides (docker history) | 8 (4 héritées de l'image de base + 4 du Dockerfile) |
| Couche la plus volumineuse | Couche de base Debian : 101 MB ; parmi nos couches : RUN pip install, 48,9 MB |

Vérifications : `docker run --rm vitals-producer:0.1 id` → `uid=10001(appuser)` ; `User=appuser Cmd=[python producer.py]`.

### Mesure 2
| Scénario de rebuild | Durée / étapes CACHED |
|---|---|
| Aucune modification | ≈ 0,3 s / 5 CACHED (toutes les étapes du Dockerfile) |
| Modification de producer.py | 0,37 s / 3 CACHED (WORKDIR, COPY requirements, pip install) |
| Modification de requirements.txt | 2,67 s / 1 CACHED (WORKDIR) — pip install refait (2,2 s) |

**Q1.2** — Avec un unique `COPY . ./` placé avant le `RUN pip install`, c'est le scénario « modification de producer.py » qui changerait : toute modification du code invaliderait la couche COPY, donc aussi l'installation des dépendances et toutes les couches suivantes. Sa durée passerait d'environ 0,4 s à environ 2,7 s (comme le scénario requirements.txt), les deux autres scénarios restant inchangés.

---

## Partie 2 — Docker Compose

### Décisions
| Décision | Valeur retenue |
|---|---|
| Volume nommé des journaux | `vitals-logs` (nom réel : `s1-vitals-stream_vitals-logs`) |
| Réseau applicatif | `vitals-net`, driver bridge (nom réel : `s1-vitals-stream_vitals-net`) |
| VITALS_TOPIC | `mimic-vitals` |
| healthcheck interval | `10s` : test léger, inutile de le lancer chaque seconde ; une panne est détectée en ~30 s |
| healthcheck retries | `3` : tolère un échec ponctuel sans laisser un service mort « healthy » plus de ~30 s |

Mountpoint du volume : `/var/lib/docker/volumes/s1-vitals-stream_vitals-logs/_data` (dans la VM Linux de Docker Desktop, pas directement sur le disque du Mac).
Résolution DNS : `nslookup producer` sur le réseau du projet → `172.18.0.2`.

**Q2.1** — Volume nommé plutôt que bind mount :
1. **Portabilité / reproductibilité** : le volume est géré par Docker et ne dépend ni de l'arborescence ni de l'OS de l'hôte ; le même fichier Compose fonctionne à l'identique sur tous les postes.
2. **Droits et performances** : le volume hérite des droits de `/app/logs` défini dans l'image (propriétaire UID 10001), alors qu'un bind mount appartiendrait à l'utilisateur du Mac et l'utilisateur non-root du conteneur risquerait de ne pas pouvoir y écrire. Sur macOS, il évite aussi le coût du partage de fichiers hôte ↔ VM.

### Mesure 3
Délai entre le démarrage et healthy : **≈ 5,1 s** (StartedAt 09:41:53,90 → premier test réussi 09:41:58,96). Pendant `start_period`, Docker sonde au rythme de `start_interval` (5 s par défaut), pas de `interval` ; les tests suivants s'enchaînent ensuite toutes les 10 s.

---

## Partie 3 — Kafka en mode KRaft

### Décisions
| Décision | Valeur retenue |
|---|---|
| Port hôte publié en face de 29092 | `29092` |
| KAFKA_PROCESS_ROLES | `broker,controller` (un seul nœud porte les deux rôles) |
| Facteur de réplication des topics internes | `1` (un seul broker) |
| KAFKA_AUTO_CREATE_TOPICS_ENABLE | `false` (une faute de frappe sur un nom de topic provoque une erreur au lieu de créer un topic fantôme) |
| Volume nommé des journaux Kafka | `kafka-data` (nom réel : `s1-vitals-stream_kafka-data`) |

### Mesure 4
Temps entre `docker compose up -d kafka` et healthy : **8,1 s** (image déjà téléchargée).

Topic `mimic-vitals` créé avec **3 partitions**, facteur de réplication **1** (`--describe` : Leader 1, Replicas 1, Isr 1 pour chaque partition).

**Q3.1** — `--replication-factor` ne peut pas valoir 2 car il n'y a qu'un seul broker : chaque réplique d'une partition doit être hébergée par un broker différent. Kafka refuse la création avec une `InvalidReplicationFactorException` : *Unable to replicate the partition 2 time(s): The target replication factor of 2 cannot be reached because only 1 broker(s) are registered.*

### Mesure 5
| Clé | Partition(s) et offsets observés |
|---|---|
| P001 | partition 2 — offsets 0, 1, 2 |
| P002 | partition 1 — offset 0 |
| P003 | partition 0 — offset 0 |

Offsets de fin : partition 0 → 1, partition 1 → 1, partition 2 → 3 (total 5).

> Remarque : dans `cp-kafka:7.8.0` (Kafka 3.8), la classe `kafka.tools.GetOffsetShell` de l'énoncé n'existe plus (`ClassNotFoundException`) ; l'outil équivalent est le script `kafka-get-offsets`, utilisé pour tous les relevés d'offsets.

**Q3.2** — Oui, les trois messages de P001 sont dans la même partition (2), car la partition est calculée à partir du hachage de la clé. Leurs offsets se suivent (0, 1, 2) dans l'ordre exact d'envoi (HR, SPO2, ABP_SYS). L'ordre est donc garanti pour un même patient, à l'intérieur de sa partition, mais pas à l'échelle du topic : P002 (partition 1), envoyé entre deux messages de P001, peut être lu avant ou après eux.

---

## Partie 4 — Producteur Python

### Décisions de configuration
| Paramètre | Valeur | Justification |
|---|---|---|
| bootstrap.servers | `KAFKA_BOOTSTRAP` | `localhost:29092` depuis l'hôte, `kafka:9092` dans Compose |
| client.id | `vitals-simulators1` | imposé |
| acks | `all` | leader + toutes les ISR confirment : durabilité maximale ; requis par l'idempotence |
| linger.ms | `5` | léger regroupement en lots, latence négligeable pour un flux ~1 Hz par patient |
| retries | `5` | imposé |
| enable.idempotence | `True` | un retry ne crée ni doublon ni inversion d'ordre |
| partitioner | `murmur2_random` | même hachage murmur2 que le client Java (Flink) : une clé va dans la même partition des deux côtés. Le défaut librdkafka (`consistent_random`, CRC32) n'est pas interopérable |
| NB_PATIENTS | `12` | 12 lits |
| INTERVAL_S | `1.0` | un moniteur ICU échantillonne à ~1 Hz |

Clé Kafka = `patient_id` encodé en UTF-8 : tous les relevés d'un patient vont dans la même partition, sa série temporelle reste ordonnée.

### Mesure 6
| Forme de depends_on | Erreurs de connexion au démarrage ? | Lignes d'erreur avant stabilisation |
|---|---|---|
| depends_on: [kafka] (testé via `condition: service_started`, sémantique identique) | Oui | 2 (`Connect to ipv4#172.18.0.3:9092 failed: Connection refused`) |
| condition: service_healthy | Non | 0 |

Avec la forme courte, Compose démarre le producteur en même temps que le conteneur kafka (0,2 s), avant que le broker écoute sur 9092. Avec `service_healthy`, Compose attend que le healthcheck de Kafka soit vert (6,7 s) avant de lancer le producteur, qui se connecte du premier coup. Dans les deux cas apparaît un avertissement (niveau 4) `GETPID … Coordinator load in progress: retrying` : ce n'est pas une erreur de connexion, mais l'obtention de l'identifiant de producteur idempotent pendant que le coordinateur finit de charger ; le client réessaie seul. C'est la forme `service_healthy` qui est conservée pour le rendu.

---

## Partie 5 — Vérification et mesure

### Mesure 7
| Grandeur | Valeur |
|---|---|
| Somme des offsets de fin avant (t0) | 492 |
| Somme des offsets de fin après 60 s (t1) | 1224 |
| Débit calculé (t1 - t0) / 60 | 12,20 msg/s |
| Débit annoncé par le producteur à l'arrêt | 11,96 msg/s (1224 messages en 102,4 s) |
| INTERVAL_S / NB_PATIENTS | 1.0 / 12 → théorique 12 msg/s |

| Grandeur | Valeur |
|---|---|
| Débit mesuré avec INTERVAL_S=0.1 | 117,6 msg/s (t0 = 113, t1 = 7169) |
| Écart avec la valeur théorique (120 msg/s) | −2,0 % |

**Q5.1** — Oui, à moins de 5 % près. Le débit annoncé (11,96 msg/s, −0,3 %) est légèrement inférieur à NB_PATIENTS / INTERVAL_S, car la période réelle d'un tour vaut INTERVAL_S + le temps de travail du tour (12 produce, sérialisation JSON, poll) : le sleep s'ajoute au travail au lieu de le compenser. Le débit calculé par les offsets (12,20 msg/s, +1,7 %) est un peu au-dessus, car chaque relevé `kafka-get-offsets` via `docker compose exec` prend environ 1 s : la fenêtre réelle entre t0 et t1 dépasse 60 s. Avec INTERVAL_S = 0.1, le surcoût par tour pèse relativement 10 fois plus et la granularité de `time.sleep` s'y ajoute : l'écart monte à 2 %. Pour un débit exact, il faudrait dormir jusqu'à une échéance absolue (début + n × INTERVAL_S).

### Mesure 8
| Observation | Valeur relevée |
|---|---|
| Le processus producteur s'arrête-t-il ? | Non, il continue de tourner (`State.Running = true`) |
| Message d'erreur exact (première ligne) | `FAIL … kafka:9092/1: Disconnected (after 15712ms in state UP)` |
| Délai entre stop et la première erreur | ≈ 1 s (stop à 08:04:10 UTC, erreur à 08:04:11,06) |
| Après start, la production reprend-elle sans relancer le script ? | Oui : offsets 2184 → 2316 en 10 s (rattrapage du buffer puis rythme normal) |
| Des messages produits pendant la coupure ont-ils été perdus ? | Non : 0 échec de livraison, 0 message resté dans le buffer ; le topic contient 2328 messages = 1224 + 1104 envoyés |

Séquence observée pendant la coupure : `Disconnected` → `Connection refused` → `Connection setup timed out` → `Failed to resolve 'kafka:9092'` (le conteneur arrêté disparaît du DNS du réseau).

**Q5.2** — Entre `stop` et `start`, le producteur ne s'arrête pas : `produce()` continue de déposer les relevés dans le buffer local de librdkafka, et les envois impossibles restent en file d'attente au lieu d'être abandonnés. Avec `acks=all`, un message n'est considéré comme livré qu'après l'accusé du leader et des ISR, donc rien n'est retiré du buffer sans confirmation ; `retries=5` permet de réémettre les lots dont l'envoi a échoué en cours de route. `enable.idempotence=True` associe à chaque message un identifiant de producteur et un numéro de séquence : au retour du broker, les réémissions ne créent ni doublon ni inversion d'ordre dans une partition, ce que confirme le total exact de 2328 messages. La durée de survie sans broker est bornée par `message.timeout.ms` (`delivery.timeout.ms`, 300 s par défaut), au-delà duquel les messages en attente sont déclarés en échec via le callback, et par la capacité du buffer local (`queue.buffering.max.messages` / `max.kbytes`), qui, une fois pleine, fait lever `BufferError` à `produce()`.

---

## Nettoyage

**Q6.1** — `docker compose down` supprime les conteneurs et le réseau mais **conserve les volumes nommés** : au prochain `up`, Kafka retrouve son répertoire de données (`kafka-data`), le topic `mimic-vitals` et tous ses messages et offsets sont toujours là. `docker compose down -v` supprime **aussi les volumes nommés** déclarés dans le fichier : au prochain `up`, le broker repart d'un répertoire vide, le topic n'existe plus et, la création automatique étant désactivée, il faut le recréer avec `kafka-topics --create` avant de relancer le producteur.

# Mesures — séance 2

Ingestion et persistance : consommateur, consumer groups et PostgreSQL.
Toutes les valeurs viennent des sorties brutes de `captures_s2/` (une par partie), produites le 27/10/2026 par `s2.sh`.

## Adaptations à mon dossier de la séance 1

- Mon broker utilise l'image `confluentinc/cp-kafka:7.8.0`. Les outils s'appellent donc `kafka-topics`, `kafka-consumer-groups`, `kafka-get-offsets` et `kafka-console-consumer`, et non `/opt/kafka/bin/*.sh`. Les raccourcis `KT`, `KG` et `KO` ont été adaptés en conséquence.
- Mon producteur de la séance 1 est construit sur `python:3.12-slim`. Le consommateur suit l'image imposée par l'énoncé, `cgr.dev/chainguard/wolfi-base`, figée par digest : `sha256:0b179206ab162b12a43bacea1b525a4023e0272005c92d08555c61b1beaedc40`.
- `INTERVAL_S` n'existait pas dans mon Compose. Je l'ai ajouté sous la forme `"${INTERVAL_S:-1.0}"` (partie 5.2).

## Incidents rencontrés pendant la séance

1. **Disque plein.** Pendant le premier lancement, le disque du poste était saturé (`No space left on device`). Docker Desktop a planté et l'image `cp-kafka` retéléchargée à ce moment-là est restée corrompue (`unable to find user appuser: no matching entries in passwd file`). J'ai supprimé l'image puis je l'ai retéléchargée. Le volume `kafka-data` n'a pas été touché.
2. **Droits de lecture du code.** Le premier conteneur consommateur bouclait sur `can't open file '/app/consumer.py': [Errno 13] Permission denied`. Les fichiers copiés depuis le Mac étaient en `600`, donc illisibles par `appuser`. Correction : `RUN chmod -R a+rX /app` avant `USER appuser`.
3. **Contrôle `docker scout cves` non fait.** Il est prévu avant un `docker push`, et l'image n'est publiée nulle part.

## Vérification des prérequis

| Contrôle | Valeur relevée |
|---|---|
| Sortie de `docker compose config --quiet` | vide (compose valide) |
| Le volume `kafka-data` existe-t-il encore ? | oui |
| Version de PostgreSQL installée nativement | aucune |

---

## Partie 1 — Passage à plusieurs partitions

### Mesure 1 — état de départ
| Grandeur | Valeur relevée |
|---|---|
| Nombre de partitions de `mimic-vitals` avant modification | 3 |
| Somme des offsets de fin | 3 000 |

### Mesure 2 — répartition avant modification
| Partition | Patients observés | Nombre de patients |
|---|---|---|
| 0 | P003, P005, P008, P009, P010, P012 | 6 |
| 1 | P002, P006, P007, P011 | 4 |
| 2 | P001, P004 | 2 |

### Décision
`NB_PARTITIONS=6`. 6 est divisible par 2 et par 3 : avec 2 consommateurs, chacun reçoit 3 partitions, et avec 3 consommateurs, chacun en reçoit 2. Aucun ne reste inactif.

### Mesure 3 — répartition après modification (messages produits après le `--alter`)
| Partition | Patients observés | Nombre de patients |
|---|---|---|
| 0 | P005, P010, P012 | 3 |
| 1 | P007 | 1 |
| 2 | aucun | 0 |
| 3 | P003, P008, P009 | 3 |
| 4 | P002, P006, P011 | 3 |
| 5 | P001, P004 | 2 |

**Q1.1** — Non : 8 patients sur 12 ont changé de partition (par exemple P001 est passé de 2 à 5), seuls P005, P007, P010 et P012 sont restés en place. La partition vaut `murmur2(clé) mod N` : en passant de N = 3 à N = 6, chaque patient de l'ancienne partition *p* tombe en *p* ou en *p* + 3, puisque 6 est un multiple de 3, et c'est exactement ce que montrent les mesures 2 et 3.

**Q1.2** — Non, elle n'est pas uniforme (3, 1, 0, 3, 3, 2) et la partition 2 ne reçoit plus aucun patient. Un hachage place chaque clé au hasard de façon indépendante : avec seulement 12 clés pour 6 cases, obtenir exactement 2 clés par partition est très improbable, et seule une loi des grands nombres, avec des milliers de clés, lisserait la charge. En partie 3, les consommateurs auront le même nombre de partitions mais pas le même nombre de patients, donc pas la même charge, et celui qui hérite de la partition 2 n'en lira rien.

**Q1.3** — La garantie d'ordre par patient est cassée : les messages de P001 écrits avant le changement sont dans la partition 2, les suivants dans la partition 5, et un consommateur peut lire un relevé récent de P001 avant un relevé plus ancien. La fenêtre s'étend du `--alter` jusqu'au moment où tous les anciens messages de la partition 2 ont été consommés ; pour quiconque rejoue l'historique, elle dure aussi longtemps que la rétention conserve ces anciens messages.

### Contrôle 1.4
6 partitions, toutes avec `Leader: 1`. La somme des offsets de fin passe de 3 000 à 3 324. Le producteur tourne en arrière-plan : `intervalle=1.0s`.

---

## Partie 2 — Offsets et acquittement

Décision pour `auto.offset.reset` dans `lecteur.py` : la valeur de la variable `AUTO_OFFSET_RESET` (`OFFSET_RESET`), `earliest` par défaut. Ainsi, un groupe neuf ne perd rien de ce qui est encore dans le topic, et l'expérience 1 peut comparer les deux valeurs sans toucher au code.

### Mesure 4
| Groupe | auto.offset.reset | 1er lancement (20 s) | 2e lancement (20 s) |
|---|---|---|---|
| explo-earliest | earliest | 2 341 | 264 |
| explo-latest | latest | 229 | 252 |

### Mesure 5 — `--describe explo-earliest` après arrêt propre (partition 0)
| Colonne | Valeur relevée |
|---|---|
| CURRENT-OFFSET | 2 187 |
| LOG-END-OFFSET | 2 193 |
| LAG | 6 (messages produits après l'arrêt) |
| CONSUMER-ID | `-` (« has no active members ») |

**Q2.1** — À l'arrêt, le lecteur a commité ses offsets dans `__consumer_offsets` : au deuxième lancement, le groupe reprend à ce curseur et ne lit que les 264 messages arrivés depuis. `auto.offset.reset` ne sert que lorsqu'aucun offset commité n'existe pour la partition, ce qui n'était le cas qu'au tout premier lancement d'un groupe neuf.

**Q2.2** — Première façon : lancer la nouvelle version sous un **nouveau `group.id`** avec `earliest`, qui part du début sans toucher aux offsets du groupe existant. Deuxième façon : **réinitialiser les offsets** d'un groupe avec `kafka-consumer-groups --reset-offsets --to-earliest --execute` (groupe arrêté), ou se positionner explicitement avec `assign()`/`seek()`. En production, je choisirais le nouveau groupe : il est sans effet sur le groupe en service, réversible, et les deux traitements peuvent tourner côte à côte pour être comparés.

### Mesure 6 — arrêt brutal (`kill -9`) après 15 s
| Groupe | Mode | Lus avant le kill (dernier `lus=`) | CURRENT-OFFSET total après le kill | Relus au redémarrage |
|---|---|---|---|---|
| explo-auto | automatique | 3 300 | 6 324 | ≈ 36 |
| explo-manuel | manuel (tous les 100) | 4 800 | 7 800 | ≈ 12 |

Les messages relus sont ceux qui avaient été lus mais pas encore acquittés, c'est-à-dire le LAG relevé juste après le kill (offset de fin 6 360 pour 6 324 commités, et 7 812 pour 7 800). La relance n'a reçu ses partitions qu'au bout de 42,4 s et 43,4 s : c'est l'expiration de la session du membre tué. En mode manuel, la relecture peut aller jusqu'à 99 messages ; ici, le kill est tombé juste après un acquittement (`lus=4800`, multiple de 100).

**Q2.3** — En **acquittement automatique**, le client commite périodiquement ce qui a été *remis* à l'application, que le traitement soit fait ou non : si le processus meurt après ce commit mais avant la fin du traitement, le message est perdu. En **acquittement manuel après traitement**, le commit arrive après le traitement : une mort entre les deux fait relire et retraiter le message, d'où un doublon. Le risque de perte vient donc d'un acquittement placé *avant* (ou indépendamment de) la fin du traitement, et le risque de doublon d'un acquittement placé *après*. Le lecteur ne traite rien, donc les deux modes ont seulement relu ici ; mais c'est l'acquittement manuel *après* l'écriture qui ne perd jamais rien, et c'est lui que la partie 4 retient, la clé primaire neutralisant les doublons.

---

## Partie 3 — Consumer groups et rééquilibrage

### Mesure 7 — attribution (stratégie `range`)
| Situation | Partitions de cA | Partitions de cB | Partitions de cC |
|---|---|---|---|
| cA seul | 0, 1, 2, 3, 4, 5 | — | — |
| cA + cB | 0, 1, 2 | 3, 4, 5 | — |
| cA + cB + cC | 0, 1 | 2, 3 | 4, 5 |

Les données du broker (`--describe`) sont cohérentes avec ce tableau : chaque partition a un seul CONSUMER-ID.

### Mesure 8 — 60 s à trois
| Instance | Messages lus pendant les 60 s | Patients couverts |
|---|---|---|
| cA | 269 | 4 (P005, P007, P010, P012) |
| cB | 218 | 3 (P003, P008, P009) |
| cC | 390 | 5 (P001, P002, P004, P006, P011) |

**Q3.1** — Non : chaque instance a 2 partitions, mais cC en lit presque deux fois plus que cB. Le débit d'une instance est proportionnel au nombre de patients de ses partitions (mesure 3) : 5 pour cC, 4 pour cA, et seulement 3 pour cB, dont la partition 2 est vide. Chaque patient émet environ 1 relevé par seconde.

### Mesure 9 — un consommateur de trop
| Grandeur | Valeur relevée |
|---|---|
| Nombre de partitions du topic | 6 |
| Nombre d'instances lancées | 7 |
| Instances affichant `partitions=[aucune]` | 1 |
| `lus=` de cette instance après 30 s | 0 |

Une deuxième instance, s3, était inutile en pratique : elle détenait la partition 2, qui est vide, et n'a lu aucun message en 30 s.

**Q3.2** — Le parallélisme de consommation d'un groupe est borné par le nombre de partitions (ici 6), car une partition n'est attribuée qu'à un seul membre. Pour aller au-delà, il faut augmenter le nombre de partitions du topic. Comme en question 1.3, les clés changent alors de partition pour les nouveaux messages, et l'ordre par patient n'est plus garanti tant que les anciens messages n'ont pas été consommés.

### Mesure 10 — perte d'un consommateur
| Scénario | Délai avant réattribution à cA | `session.timeout.ms` par défaut |
|---|---|---|
| A — Ctrl-C (arrêt propre, `close()` appelé) | 3,9 s | 45 000 ms |
| B — Ctrl-Z puis `kill -9` (arrêt brutal) | 46,0 s | idem |

**Q3.3** — Avec `close()`, le consommateur envoie une demande explicite de départ (LeaveGroup) : le coordinateur relance aussitôt le rééquilibrage, en 3,9 s. Après un `kill -9`, aucun message de départ n'est envoyé : le coordinateur ne s'en aperçoit qu'en constatant l'absence de heartbeats pendant `session.timeout.ms`, d'où 46,0 s pour 45 s configurées. Un délai d'une seconde exclurait des membres pourtant sains à la moindre pause (ramasse-miettes, réseau chargé, poste qui rame), déclenchant des rééquilibrages en cascade pendant lesquels personne ne consomme. Le broker refuse d'ailleurs les valeurs inférieures à `group.min.session.timeout.ms`, soit 6 s par défaut.

### Mesure 11 — stratégie d'attribution, départ propre de cB
| Stratégie | Lignes RETRAIT affichées par cA lors du départ de cB | Partitions listées dans ce RETRAIT |
|---|---|---|
| range | 1 | 0, 1, 2 (toutes les siennes), puis ATTRIBUTION de 0 à 5 |
| cooperative-sticky | 0 | — (ATTRIBUTION incrémentale de 2, 1, 0 seulement, 1,4 s après le départ) |

**Q3.4** — En stratégie coopérative, le consommateur garde les partitions qui ne changent pas de main : il continue d'écrire et d'acquitter ses lots sur celles-ci pendant le rééquilibrage, au lieu de tout rendre. Le lot en cours n'est donc perturbé que pour les partitions qui bougent, ce qui réduit l'arrêt de l'ingestion et les relectures (doublons absorbés par la clé primaire) à ces seules partitions.

**Stratégie retenue dans le code de rendu** : `lecteur.py` utilise `range` par défaut, comme le demande l'énoncé, réglable par `PARTITION_STRATEGY`. `consumer.py` utilise `cooperative-sticky`, comme imposé en partie 4.

---

## Partie 4 — Persistance PostgreSQL

Clé primaire : `pk_vitals_raw PRIMARY KEY (kafka_partition, kafka_offset)`.

**Q4.1** — La paire (partition, offset) identifie un message Kafka de façon unique et stable : rejouer le même message redonne exactement la même clé, et le doublon est absorbé, alors que deux messages différents n'auront jamais la même clé. Avec `(patient_id, event_time, vital_type)`, deux relevés légitimes peuvent avoir la même clé. Par exemple, un moniteur qui renvoie une fréquence cardiaque corrigée après recalibrage dans la même milliseconde, ou deux relevés de même horodatage après un recalage d'horloge (NTP qui revient en arrière) : le second serait silencieusement écarté par `ON CONFLICT DO NOTHING`, alors que c'est une donnée réelle.

### Mesure 12
| Grandeur | Valeur relevée |
|---|---|
| Délai entre `up -d postgres` et `healthy` | ≈ 5 s (serveur prêt 1,6 s après le démarrage, puis `healthy` au premier test `pg_isready`) |
| Nombre de tables listées par `\dt` | 1 (`vitals_raw`) |
| Nom de la contrainte de clé primaire | `pk_vitals_raw` |

La première mesure a affiché `TIMEOUT` à tort : un défaut du script de mesure, qui comparait des décimales écrites avec une virgule. La valeur ci-dessus a été retrouvée dans les journaux horodatés du conteneur.

### Décisions du consommateur
| Décision | Valeur |
|---|---|
| Clause de conflit | `ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING` |
| Mode d'acquittement | `enable.auto.commit: False` (commit synchrone après `inserer_lot()`) |
| Taille de lot | 200 |
| Délai de départ d'un lot non plein | 5,0 s |
| Point de départ d'un groupe neuf | earliest |

### Mesure 13 — consommateur lancé depuis l'hôte
| Grandeur | Valeur relevée |
|---|---|
| `lus=` à l'arrêt | 16 585 |
| `inseres=` à l'arrêt | 16 585 (0 doublon, 0 rejet ; 352,9 msg/s, rattrapage de l'historique) |
| `lignes_totales` | 16 585 |
| `messages_distincts` | 16 585 |
| `patients_distincts` | 12 |

Les partitions 0, 1 et 2 commencent aux offsets 1 500, 1 000 et 500, et non à 0 : les messages de la séance 1 (9 octobre) ont été supprimés par la rétention de Kafka (7 jours par défaut). C'est une illustration directe du « flux volatil » décrit dans le contexte, et de la raison d'être de PostgreSQL.

**Q4.2** — Oui, ils sont égaux (16 585) : aucun message Kafka n'a donné deux lignes. S'ils ne l'étaient pas, la clé primaire ne jouerait pas son rôle (absente ou mal définie) et la chaîne aurait écrit des doublons, ce qui fausserait les moyennes et déclencherait des alertes injustifiées.

### Mesure 14 — chaîne complète
| Grandeur | Valeur relevée |
|---|---|
| Nombre de services affichés par `docker compose ps` | 4 (kafka, postgres, producer, consumer) |
| Services en état healthy | 3 (kafka, postgres, producer ; consumer n'a pas de healthcheck) |
| Taille moyenne des lots (`LOT taille=`) | 61,9 (12 lots sur 60 s) |
| Croissance de `lignes_totales` sur 60 s | +743 lignes |

**Q4.3** — Le producteur émet 12 messages par seconde (11,93 msg/s mesurés en partie 1, et 743 lignes par minute ici). Remplir un lot de 200 prendrait 200 / 12 ≈ 16,7 s, plus que les 5 s de `BATCH_TIMEOUT_S` : c'est donc toujours le **délai** qui déclenche l'envoi. Un lot contient alors environ 5 × 12 = 60 messages, ce qui correspond aux 61,9 mesurés. Seul le rattrapage d'un retard produit des lots pleins de 200.

---

## Partie 5 — Résilience et mesure

### Mesure 15 — lag en régime nominal
| Instant | Somme des LAG |
|---|---|
| t0 | 23 |
| t0 + 30 s | 12 |
| t0 + 60 s | 11 |

**Q5.1** — Le lag est stable : il oscille sans tendance. À l'équilibre, il vaut entre 0, juste après un commit, et environ 60, juste avant le suivant (5 s × 12 msg/s non encore acquittés), soit une trentaine en moyenne. C'est l'ordre de grandeur mesuré (11 à 23).

### Mesure 16 — surcharge (`INTERVAL_S=0.005`, `BATCH_SIZE=1`, consommateur bridé à 0,05 CPU)
Débit du producteur mesuré : **1 856 msg/s** (2 400 théoriques).

| Instant | Somme des LAG | Écart avec le relevé précédent |
|---|---|---|
| t0 | 13 258 | — |
| t0 + 30 s | 69 422 | +56 164 |
| t0 + 60 s | 124 389 | +54 967 |
| t0 + 120 s | 234 421 | +110 032 (+54 605 puis +55 427) |

Le lag croît d'environ 1 840 messages par seconde : le consommateur bridé n'en traite qu'une quinzaine par seconde.

### Mesure 17 — passage à deux consommateurs
| Grandeur | Valeur relevée |
|---|---|
| Somme des LAG juste avant le `--scale` | 238 837 |
| Somme des LAG 60 s après | 342 219 |
| Nombre de membres du groupe | 2 (3 partitions chacun) |
| Le lag décroît, se stabilise ou continue de croître ? | Il **continue de croître** (environ +1 700 msg/s) |

**Q5.2** — Dans l'ordre : (1) le coût de traitement par message côté consommateur, ici un `INSERT` et un commit par message avec `BATCH_SIZE=1` et 5 % de CPU, qu'on corrige en regroupant par lots et en donnant de la CPU ; (2) la répartition et le nombre de partitions, puisque deux consommateurs ne peuvent dépasser la limite des 6 partitions et que la partition 2 vide ne porte aucune charge ; (3) la capacité d'écriture de PostgreSQL, ressource partagée par tous les consommateurs. C'est cette dernière qui ne se corrige pas en ajoutant des consommateurs : s'ils attendent tous la même base, en ajouter ne fait qu'allonger la file d'attente devant elle.

**Retour à l'état nominal** : `INTERVAL_S=1.0`, une seule instance, `NanoCpus=0` (plus de bridage). Le retard de 316 791 messages a été rattrapé en moins de 17 s (lag = 23).

### Mesure 18 — PostgreSQL coupé pendant 90 s
| Observation | Valeur relevée |
|---|---|
| Le conteneur consommateur s'arrête-t-il ou redémarre-t-il ? | Il ne s'arrête pas. Il retente la connexion 30 fois toutes les 2 s, puis le processus s'arrête au bout de 60 s (`ERREUR FATALE`) et Docker le **redémarre** (`restart: unless-stopped`, 1 redémarrage) |
| Première ligne d'erreur affichée | `ERREUR PostgreSQL pendant l'ecriture : server closed the connection unexpectedly -> lot de 63 lignes CONSERVE, offsets NON acquittes` |
| Somme des LAG après 90 s de coupure | 1 271 (de 263 à +15 s jusqu'à 1 247 à +90 s, soit 12 msg/s) |
| `lignes_totales` avant la coupure | 408 505 |
| `lignes_totales` 60 s après le redémarrage de PostgreSQL | 410 473 (+1 968 : le retard plus le flux courant) |
| Somme des LAG 60 s après le redémarrage | 47 |
| Le consommateur a-t-il rattrapé son retard sans intervention ? | Oui |

**Q5.3** — Non, les offsets n'ont pas avancé : le lag a crû régulièrement d'environ 12 messages par seconde pendant toute la coupure. Dans `consumer.py`, `commit()` n'est appelé qu'après le retour sans erreur de `inserer_lot()` : comme l'insertion échouait, aucun acquittement n'a eu lieu, et les 63 lignes en mémoire comme tout ce qui est arrivé ensuite ont été relus et écrits au retour de la base. Dans l'ordre inverse, les offsets auraient avancé sur des lots jamais écrits, et environ 1 100 relevés (90 s × 12 msg/s) auraient été perdus définitivement.

### Mesure 19 — `SIGKILL` du consommateur puis reprise
| Grandeur | Valeur relevée |
|---|---|
| `lignes_totales` avant le SIGKILL | 410 533 |
| `inseres=` de l'instance relancée (90 s, ligne `Arret.`) | 1 128 |
| `ignores_doublons=` de l'instance relancée | 0 |
| Lignes renvoyées par la requête de doublons | 0 |
| Partitions où lignes ≠ étendue (trou) | aucune (6 partitions sur 6 sans trou) |

État final : 411 661 lignes = 411 661 messages distincts, 12 patients. Observation 90 s au lieu de 60 s : la relance attend environ 45 s l'expiration de la session de l'instance tuée.

**Q5.4** — Ici, `ignores_doublons` vaut 0 : le `SIGKILL` est tombé pendant l'accumulation d'un lot, avant son écriture. Les messages non acquittés n'avaient donc jamais été écrits, et la relance les a insérés pour la première fois. Il n'y a de doublon que si l'arrêt survient entre `inserer_lot()` et `commit()` (une fenêtre de quelques millisecondes) ou lors d'un rééquilibrage : le lot déjà en base est alors relu, et `ignores_doublons > 0` prouve que la clé primaire a absorbé cette relecture au lieu de créer une seconde ligne. C'est le comportement voulu, pas un défaut. L'absence de doublons et de trous (requêtes ci-dessus) reste la preuve qui compte.

**Q5.5** — Sur le flux, la chaîne garantit **at-least-once** : un message n'est acquitté qu'après son écriture, donc il peut être relivré, mais jamais perdu. Dans la table, l'état final est équivalent à **exactly-once** : chaque message produit exactement une ligne, grâce à l'idempotence de l'insertion (clé primaire et `ON CONFLICT DO NOTHING`), sans recourir aux transactions Kafka. Seule exception : un message malformé est volontairement acquitté sans être écrit, donc at-most-once pour lui. Une file d'attente des messages rejetés (extension 1) permettrait de le conserver.

---

## Nettoyage

**Q6.1** — `docker compose down -v` supprime le volume `kafka-data` : le topic, ses messages et `__consumer_offsets`, donc les offsets commités de `vitals-persister`, disparaissent. Il supprime aussi `pg-data` : la table et ses lignes disparaissent. Au prochain `up`, `init.sql` est rejoué sur un volume vide, et le topic doit être recréé, puisque `KAFKA_AUTO_CREATE_TOPICS_ENABLE` vaut `false`. Le consommateur, qui n'a plus aucun offset, applique `earliest` et repart de l'offset 0 du nouveau topic : il réécrit tout depuis le début, sans doublon puisque la table est vide.

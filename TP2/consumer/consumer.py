"""Consommateur de persistance HealthStream : topic mimic-vitals -> table vitals_raw.

Ordre critique : inserer_lot() d'abord, commit() Kafka ensuite.
Un incident entre les deux provoque au pire la relecture d'un lot deja ecrit,
que la cle primaire (kafka_partition, kafka_offset) absorbe.

    KAFKA_BOOTSTRAP=localhost:29092 PG_HOST=localhost PG_PORT=55432 python consumer/consumer.py
"""
import json
import math
import os
import signal
import socket
import sys
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
VITALS_TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "vitals-persister")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
BATCH_TIMEOUT_S = float(os.getenv("BATCH_TIMEOUT_S", "5.0"))
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "55432"))
PG_DB = os.getenv("PG_DB", "healthstream")
PG_USER = os.getenv("PG_USER", "hs_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "hs_password")

TYPES_VALIDES = {"HR", "SPO2", "ABP_SYS"}
VALEUR_MAX = 999999.99  # NUMERIC(8, 2)

# execute_values remplace %s par tous les tuples du lot : une instruction, un aller-retour.
SQL_INSERT = (
    "INSERT INTO vitals_raw "
    "(kafka_partition, kafka_offset, patient_id, event_time, vital_type, value, unit) "
    "VALUES %s "
    "ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING"
)

arret_demande = False


def gerer_signal(signum, frame):
    global arret_demande
    arret_demande = True


def log(texte):
    print(f"{time.strftime('%H:%M:%S')} {texte}", flush=True)


def _premiere_ligne(exc):
    texte = str(exc).strip()
    return texte.splitlines()[0] if texte else type(exc).__name__


def creer_consommateur() -> Consumer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "client.id": f"persister-{socket.gethostname()}",
        "auto.offset.reset": AUTO_OFFSET_RESET,
        # Acquittement manuel : l'offset n'avance qu'apres l'ecriture en base.
        "enable.auto.commit": False,
        # Reequilibrage incremental : on ne rend que les partitions qui changent de main.
        "partition.assignment.strategy": "cooperative-sticky",
        # Doit rester superieur au temps d'ecriture d'un lot, retentatives comprises (30 x 2 s).
        "max.poll.interval.ms": 300000,
    }
    return Consumer(conf)


def connecter_postgres(tentatives: int = 30, delai_s: float = 2.0):
    """Connexion avec retentatives : la base peut tomber en cours de route (partie 5.3)."""
    derniere_erreur = None
    for tentative in range(1, tentatives + 1):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                    user=PG_USER, password=PG_PASSWORD, connect_timeout=3)
            conn.autocommit = False  # transactions explicites
            if tentative > 1:
                log(f"PostgreSQL de nouveau joignable (tentative {tentative})")
            return conn
        except psycopg2.OperationalError as exc:
            derniere_erreur = exc
            log(f"ERREUR PostgreSQL injoignable (tentative {tentative}/{tentatives}) : "
                f"{_premiere_ligne(exc)}")
            time.sleep(delai_s)
    raise derniere_erreur


def ligne_depuis_message(msg) -> tuple:
    """Message Kafka -> tuple d'insertion.

    Laisse remonter JSONDecodeError, KeyError, ValueError et TypeError sur un message
    malforme : l'appelant les intercepte pour ne pas faire tomber le lot entier.
    """
    releve = json.loads(msg.value())
    patient_id = releve["patient_id"]
    vital_type = releve["vital_type"]
    unit = releve["unit"]
    if not isinstance(patient_id, str) or not 0 < len(patient_id) <= 16:
        raise ValueError(f"patient_id invalide : {patient_id!r}")
    if vital_type not in TYPES_VALIDES:
        raise ValueError(f"vital_type inconnu : {vital_type!r}")
    if not isinstance(unit, str) or not 0 < len(unit) <= 8:
        raise ValueError(f"unit invalide : {unit!r}")
    valeur = float(releve["value"])
    if not math.isfinite(valeur) or abs(valeur) >= VALEUR_MAX:
        raise ValueError(f"value hors plage : {valeur!r}")
    try:
        # event_time arrive en millisecondes epoch.
        event_time = datetime.fromtimestamp(int(releve["event_time"]) / 1000, tz=timezone.utc)
    except (OverflowError, OSError) as exc:
        raise ValueError(f"event_time invalide : {releve['event_time']!r}") from exc
    return (msg.partition(), msg.offset(), patient_id, event_time,
            vital_type, round(valeur, 2), unit)


def inserer_lot(conn, lignes: list) -> int:
    """Ecrit le lot et valide la transaction. Retourne les lignes REELLEMENT inserees."""
    if not lignes:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, SQL_INSERT, lignes, page_size=len(lignes))
        inserees = cur.rowcount  # les doublons absorbes par ON CONFLICT n'y comptent pas
    conn.commit()
    return inserees


def acquitter(consommateur):
    """commit() Kafka synchrone, toujours APRES inserer_lot()."""
    try:
        consommateur.commit(asynchronous=False)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError._NO_OFFSET:
            # Ex. reequilibrage en cours : le lot est deja en base, une relecture sera absorbee.
            log(f"ERREUR commit Kafka : {exc}")


def _fermer(conn):
    try:
        conn.close()
    except Exception:  # noqa: BLE001 - connexion deja morte
        pass


def sur_attribution(consommateur, partitions):
    log(f"ATTRIBUTION partitions=[{', '.join(str(p.partition) for p in partitions) or 'aucune'}]")


def sur_retrait(consommateur, partitions):
    log(f"RETRAIT partitions=[{', '.join(str(p.partition) for p in partitions) or 'aucune'}]")


def main() -> int:
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)
    log(f"Demarrage : bootstrap={KAFKA_BOOTSTRAP} topic={VITALS_TOPIC} groupe={CONSUMER_GROUP} "
        f"lot={BATCH_SIZE} delai={BATCH_TIMEOUT_S}s base={PG_HOST}:{PG_PORT}/{PG_DB}")

    conn = connecter_postgres()
    consommateur = creer_consommateur()
    consommateur.subscribe([VITALS_TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)

    lus = inseres = ignores = rejets = 0
    lot = []
    debut = dernier_envoi = time.time()
    code_retour = 0
    try:
        while not arret_demande:
            msg = consommateur.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log(f"ERREUR Kafka : {msg.error()}")
                else:
                    lus += 1
                    try:
                        lot.append(ligne_depuis_message(msg))
                    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
                        # Compte puis ignore ; il sera acquitte avec le lot suivant.
                        rejets += 1
                        log(f"REJET partition={msg.partition()} offset={msg.offset()} : "
                            f"{type(exc).__name__}: {exc}")

            lot_pret = bool(lot) and (len(lot) >= BATCH_SIZE
                                      or time.time() - dernier_envoi >= BATCH_TIMEOUT_S)
            if lot_pret:
                try:
                    n = inserer_lot(conn, lot)           # 1. la base d'abord
                    acquitter(consommateur)              # 2. l'acquittement ENSUITE
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                    log(f"ERREUR PostgreSQL pendant l'ecriture : {_premiere_ligne(exc)} "
                        f"-> lot de {len(lot)} lignes CONSERVE, offsets NON acquittes")
                    _fermer(conn)
                    conn = connecter_postgres()
                    continue
                inseres += n
                ignores += len(lot) - n
                log(f"LOT taille={len(lot)} inseres={n} doublons={len(lot) - n} lus={lus}")
                lot, dernier_envoi = [], time.time()

        # Arret propre : ecrire ce qui reste en memoire, acquitter, puis quitter le groupe.
        if lot:
            n = inserer_lot(conn, lot)
            acquitter(consommateur)
            inseres += n
            ignores += len(lot) - n
            log(f"LOT taille={len(lot)} inseres={n} doublons={len(lot) - n} lus={lus} (dernier lot)")
    except psycopg2.OperationalError as exc:
        log(f"ERREUR FATALE PostgreSQL : {_premiere_ligne(exc)} -> arret SANS acquitter "
            f"les {len(lot)} lignes en memoire (elles seront relues au redemarrage)")
        code_retour = 1
    finally:
        consommateur.close()
        _fermer(conn)
        duree = time.time() - debut
        debit = lus / duree if duree > 0 else 0.0
        log(f"Arret. lus={lus} inseres={inseres} ignores_doublons={ignores} rejets={rejets} "
            f"duree={duree:.1f}s debit={debit:.1f} msg/s")
    return code_retour


if __name__ == "__main__":
    sys.exit(main())

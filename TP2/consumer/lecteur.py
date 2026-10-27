"""Lecteur exploratoire HealthStream (parties 2 et 3 de la seance 2).

Lit le topic mimic-vitals et affiche ce qu'il recoit : il n'ecrit nulle part.
Sert a observer les offsets, l'acquittement et le reequilibrage d'un consumer group.

    KAFKA_BOOTSTRAP=localhost:29092 CONSUMER_GROUP=explo-1 INSTANCE=c1 python consumer/lecteur.py
"""
import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
GROUP_ID = os.getenv("CONSUMER_GROUP", "explo-1")
OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").strip().lower() in ("1", "true", "yes", "oui")
INSTANCE = os.getenv("INSTANCE", "c1")
# range par defaut (strategie eager) ; cooperative-sticky pour l'experience 3.4.
STRATEGIE = os.getenv("PARTITION_STRATEGY", "range")

arret_demande = False
lus = 0
par_partition = {}


def gerer_signal(signum, frame):
    """SIGINT (Ctrl-C) ou SIGTERM : la boucle sort d'elle-meme, close() sera appele."""
    global arret_demande
    arret_demande = True


def creer_consommateur():
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": f"lecteur-{INSTANCE}",
        # Point de depart quand le groupe n'a AUCUN offset commite sur la partition.
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": AUTO_COMMIT,
        "partition.assignment.strategy": STRATEGIE,
    }
    return Consumer(conf)


def _liste(partitions):
    return ", ".join(str(p.partition) for p in partitions) or "aucune"


def sur_attribution(consommateur, partitions):
    print(f"[{INSTANCE}] ATTRIBUTION t={time.strftime('%H:%M:%S')} lus={lus} "
          f"partitions=[{_liste(partitions)}]", flush=True)


def sur_retrait(consommateur, partitions):
    print(f"[{INSTANCE}] RETRAIT t={time.strftime('%H:%M:%S')} lus={lus} "
          f"partitions=[{_liste(partitions)}]", flush=True)


def main():
    global lus
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    consommateur = creer_consommateur()
    print(f"[{INSTANCE}] DEMARRAGE t={time.strftime('%H:%M:%S')} groupe={GROUP_ID} "
          f"reset={OFFSET_RESET} auto_commit={AUTO_COMMIT} strategie={STRATEGIE}", flush=True)
    # subscribe() n'attribue rien : l'attribution arrive au premier poll().
    consommateur.subscribe([TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)

    try:
        while not arret_demande:
            msg = consommateur.poll(1.0)  # timeout en secondes (millisecondes en Java)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # fin de partition atteinte : pas une erreur
                print(f"[{INSTANCE}] ERREUR {msg.error()}", flush=True)
                continue
            try:
                releve = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                continue
            lus += 1
            partition = msg.partition()
            par_partition[partition] = par_partition.get(partition, 0) + 1
            if lus % 50 == 0:
                print(f"[{INSTANCE}] lus={lus} partition={partition} offset={msg.offset()} "
                      f"patient={releve.get('patient_id')}", flush=True)
            if not AUTO_COMMIT and lus % 100 == 0:
                try:
                    consommateur.commit(asynchronous=False)  # bloque jusqu'a confirmation
                except KafkaException as exc:
                    print(f"[{INSTANCE}] ERREUR commit : {exc}", flush=True)
    finally:
        repartition = ", ".join(f"{p}:{n}" for p, n in sorted(par_partition.items())) or "aucune"
        print(f"[{INSTANCE}] FIN t={time.strftime('%H:%M:%S')} lus={lus} "
              f"repartition={{{repartition}}}", flush=True)
        # close() quitte explicitement le groupe : reequilibrage immediat des autres membres.
        consommateur.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Producteur de signes vitaux HealthStream -> Kafka (topic mimic-vitals)."""
import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
VITALS_TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
NB_PATIENTS = int(os.getenv("NB_PATIENTS", "12"))
INTERVAL_S = float(os.getenv("INTERVAL_S", "1.0"))

PLAGES = {
    "HR": (55, 115, "bpm"),
    "SPO2": (90, 100, "%"),
    "ABP_SYS": (95, 145, "mmHg"),
}

arret_demande = False
nb_envoyes = 0
nb_echecs = 0


def gerer_signal(signum, frame):
    global arret_demande
    arret_demande = True


def accuse_reception(err, msg):
    global nb_echecs
    if err is not None:
        nb_echecs += 1
        print(f"ECHEC livraison cle={msg.key()} : {err}", file=sys.stderr)


def creer_producteur():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "vitals-simulators1",
        "acks": "all",
        "linger.ms": 5,
        "retries": 5,
        "enable.idempotence": True,
        "partitioner": "murmur2_random",
    }
    return Producer(conf)


def generer_releve(patient_id):
    vital_type = random.choice(list(PLAGES))
    mini, maxi, unite = PLAGES[vital_type]
    return {
        "patient_id": patient_id,
        "event_time": int(time.time() * 1000),
        "vital_type": vital_type,
        "value": round(random.uniform(mini, maxi), 1),
        "unit": unite,
    }


def main():
    global nb_envoyes
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)
    producteur = creer_producteur()
    patients = [f"P{i:03d}" for i in range(1, NB_PATIENTS + 1)]
    print(f"Producteur démarré : bootstrap={KAFKA_BOOTSTRAP} "
          f"topic={VITALS_TOPIC} patients={NB_PATIENTS} intervalle={INTERVAL_S}s")
    debut = time.time()
    while not arret_demande:
        for patient_id in patients:
            releve = generer_releve(patient_id)
            try:
                producteur.produce(
                    topic=VITALS_TOPIC,
                    key=patient_id.encode("utf-8"),
                    value=json.dumps(releve).encode("utf-8"),
                    on_delivery=accuse_reception,
                )
                nb_envoyes += 1
            except BufferError:
                print("Buffer local plein, attente...", file=sys.stderr)
                producteur.poll(1)
        producteur.poll(0)
        time.sleep(INTERVAL_S)
    restants = producteur.flush(10)
    duree = time.time() - debut
    debit = nb_envoyes / duree if duree > 0 else 0.0
    print("===== BILAN =====")
    print(f"Messages envoyés : {nb_envoyes}")
    print(f"Non livrés (échecs) : {nb_echecs}")
    print(f"Restés dans le buffer après flush : {restants}")
    print(f"Durée : {duree:.1f} s")
    print(f"Débit : {debit:.2f} msg/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())

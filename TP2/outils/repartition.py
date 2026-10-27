"""Releve de la repartition cle -> partition du topic mimic-vitals (mesures 2 et 3).

    python outils/repartition.py fin                         -> offsets de fin, en JSON
    python outils/repartition.py lire [depart.json] [sortie.json]
        lit chaque partition depuis l'offset de depart (le debut si absent) jusqu'a la fin
        relevee au lancement, et affiche les patients vus dans chaque partition.

N'appartient a aucun groupe : assign() direct, aucun offset commite.
"""
import json
import os
import sys

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "lire"
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "outil-repartition",
                  "enable.auto.commit": False, "enable.partition.eof": False})
    partitions = sorted(c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions)
    bornes = {p: c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10) for p in partitions}
    if mode == "fin":
        print(json.dumps({str(p): bornes[p][1] for p in partitions}))
        c.close()
        return 0

    depart = {}
    if len(sys.argv) > 2 and sys.argv[2] != "-":
        with open(sys.argv[2]) as f:
            depart = {int(k): int(v) for k, v in json.load(f).items()}
    a_lire, affectations = set(), []
    for p in partitions:
        debut = depart.get(p, OFFSET_BEGINNING)
        reel = bornes[p][0] if debut == OFFSET_BEGINNING else debut
        if reel < bornes[p][1]:
            a_lire.add(p)
        affectations.append(TopicPartition(TOPIC, p, debut))
    c.assign(affectations)

    cles = {p: set() for p in partitions}
    messages = {p: 0 for p in partitions}
    vides = 0
    while a_lire and vides < 15:
        msg = c.poll(1.0)
        if msg is None:
            vides += 1
            continue
        if msg.error():
            continue
        p = msg.partition()
        if p in a_lire:
            cles[p].add(msg.key().decode("utf-8") if msg.key() else "-")
            messages[p] += 1
            if msg.offset() + 1 >= bornes[p][1]:
                a_lire.discard(p)
    c.close()

    print("| Partition | Patients observés | Nombre de patients | Messages lus |")
    print("|---|---|---|---|")
    for p in partitions:
        liste = ", ".join(sorted(cles[p])) or "aucun"
        print(f"| {p} | {liste} | {len(cles[p])} | {messages[p]} |")
    if len(sys.argv) > 3:
        with open(sys.argv[3], "w") as f:
            json.dump({str(p): sorted(cles[p]) for p in partitions}, f)
    return 0


if __name__ == "__main__":
    sys.exit(main())

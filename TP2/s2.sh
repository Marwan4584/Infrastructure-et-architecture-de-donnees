#!/usr/bin/env bash
# TP seance 2 HealthStream — une partie a la fois, dans l'ordre :
#   bash ~/s1-vitals-stream/s2.sh partie1     reprise S1 et passage a 6 partitions   (~3 min)
#   bash ~/s1-vitals-stream/s2.sh partie2     offsets et acquittement                 (~6 min)
#   bash ~/s1-vitals-stream/s2.sh partie3     consumer groups et reequilibrage        (~9 min)
#   bash ~/s1-vitals-stream/s2.sh partie4     PostgreSQL et consommateur              (~5 min)
#   bash ~/s1-vitals-stream/s2.sh partie5     lag, surcharge, panne, reprise          (~15 min)
#   bash ~/s1-vitals-stream/s2.sh nettoyage   sauvegarde du schema, destruction, archive
# Chaque partie ecrit sa sortie complete dans captures_s2/<partie>.log.
export LC_ALL=C LC_NUMERIC=C   # decimales avec un point (le Mac est en francais)
PARTIE="${1:?Indique la partie : partie1 ... partie5 ou nettoyage}"
cd "$(dirname "$0")" || exit 1
D=captures_s2
mkdir -p "$D"
exec > >(tee "$D/$PARTIE.log") 2>&1
trap 'kill $(jobs -p) 2>/dev/null' EXIT
[ -x /opt/homebrew/bin/brew ] && eval "$(/opt/homebrew/bin/brew shellenv)"

TOPIC=mimic-vitals
NB_PARTITIONS=6          # 6 : divisible par 2 et par 3, aucun consommateur inactif en partie 3
PY=.venv/bin/python

# ---------- raccourcis (adaptes a l'image cp-kafka de la seance 1 : pas de /opt/kafka/bin/*.sh)
kt() { docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 "$@"; }
kg() { docker compose exec -T kafka kafka-consumer-groups --bootstrap-server localhost:9092 "$@"; }
ko() { docker compose exec -T kafka kafka-get-offsets --bootstrap-server localhost:9092 "$@"; }
psql_() { docker compose exec -T postgres psql -U hs_user -d healthstream "$@"; }
titre() { echo; echo "================ $* ($(date +%H:%M:%S))"; }
now() { perl -MTime::HiRes=time -e 'printf "%.1f", time'; }
ecart() { awk -v a="$1" -v b="$(now)" 'BEGIN{printf "%.1f", b-a}'; }
somme_fin() { ko --topic "$TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }
somme_lag() { kg --describe --group "$1" 2>/dev/null | awk '$6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}'; }
membres() { kg --describe --group "$1" --members 2>/dev/null | awk -v g="$1" '$1==g' | wc -l | tr -d ' '; }
oublier_groupe() { kg --delete --group "$1" >/dev/null 2>&1; }
lignes_totales() { psql_ -tA -c 'SELECT lignes_totales FROM v_vitals_controle;' 2>/dev/null | tr -d ' '; }

attendre_healthy() {  # conteneur, max secondes -> affiche le delai
  local t0; t0=$(now)
  while :; do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" = healthy ] && { ecart "$t0"; return 0; }
    [ "$(awk -v e="$(ecart "$t0")" -v m="$2" 'BEGIN{print (e>m)}')" = 1 ] && { echo "TIMEOUT"; return 1; }
    sleep 0.5
  done
}

# ---------- lecteur exploratoire en arriere-plan
lecteur() {  # groupe reset autocommit instance fichier [strategie]
  KAFKA_BOOTSTRAP=localhost:29092 VITALS_TOPIC=$TOPIC CONSUMER_GROUP="$1" AUTO_OFFSET_RESET="$2" \
  AUTO_COMMIT="$3" INSTANCE="$4" PARTITION_STRATEGY="${6:-range}" \
    $PY consumer/lecteur.py > "$5" 2>&1 &
}
arreter() { kill -TERM "$1" 2>/dev/null; wait "$1" 2>/dev/null; }     # = Ctrl-C : close() appele
dernier_lus() { grep -o 'lus=[0-9]*' "$1" | tail -1 | cut -d= -f2; }
nb() { grep -c "$2" "$1" 2>/dev/null; }
attrib() { grep 'ATTRIBUTION' "$1" | tail -1 | sed 's/.*partitions=//'; }
lus_attrib() { grep 'ATTRIBUTION' "$1" | tail -1 | grep -o 'lus=[0-9]*' | cut -d= -f2; }
nb_parts() { grep 'ATTRIBUTION' "$1" | tail -1 | sed -e 's/.*partitions=\[//' -e 's/\].*//' | tr ',' '\n' | grep -c '[0-9]'; }
attendre() {  # fichier motif nombre_min timeout
  local i=0
  while [ "$(nb "$1" "$2")" -lt "$3" ] && [ $i -lt $(( $4 * 2 )) ]; do sleep 0.5; i=$((i + 1)); done
}
attendre_complet() {  # fichier nb_attrib_avant nb_parts timeout -> 0 si atteint
  local i=0
  while [ $i -lt $(( $4 * 4 )) ]; do
    if [ "$(nb "$1" ATTRIBUTION)" -gt "$2" ] && [ "$(nb_parts "$1")" -ge "$3" ]; then return 0; fi
    sleep 0.25; i=$((i + 1))
  done
  return 1
}
producteur_local() {  # fichier
  KAFKA_BOOTSTRAP=localhost:29092 VITALS_TOPIC=$TOPIC $PY producer/producer.py > "$1" 2>&1 &
  local p=$!; sleep 15; kill -TERM $p; wait $p 2>/dev/null
  grep -E "envoyés|Débit" "$1"
}

echo ">> s2.sh $PARTIE — $(date '+%Y-%m-%d %H:%M:%S')"
LIBRE_GO=$(df -g "$HOME" | awk 'NR==2 {print $4}')
echo ">> Espace disque libre : ${LIBRE_GO} Go"
if [ "${LIBRE_GO:-0}" -lt 3 ]; then
  echo "ECHEC : moins de 3 Go libres (l'enonce en demande 10). Libere de l'espace puis relance."; exit 1
elif [ "${LIBRE_GO:-0}" -lt 10 ]; then
  echo ">> ATTENTION : moins de 10 Go libres, risque de disque plein (images PostgreSQL et consommateur)"
fi
docker_ok() { perl -e 'alarm 15; exec @ARGV' docker info >/dev/null 2>&1; }   # 15 s max, jamais bloquant
if ! docker_ok; then
  echo ">> Docker ne repond pas : ouverture de Docker Desktop (jusqu'a 3 min)..."; open -a Docker
  for _ in $(seq 36); do docker_ok && break; sleep 5; done
fi
docker_ok || { echo "ECHEC : Docker ne repond pas. Ouvre Docker Desktop, attends 'Engine running', puis relance."; exit 1; }
echo ">> Docker OK"
[ -x "$PY" ] || python3 -m venv .venv

case "$PARTIE" in
# =====================================================================
reparer)
  # Images abimees par le disque plein : on les supprime et on les retelecharge.
  # Le volume kafka-data (topic de la seance 1) n'est PAS touche.
  titre "Reparation des images Docker"
  docker compose down --remove-orphans
  for img in confluentinc/cp-kafka:7.8.0 vitals-producer:0.1 vitals-consumer:0.1; do
    docker rmi -f "$img" >/dev/null 2>&1 && echo "supprimee : $img"
  done
  docker image prune -f
  docker builder prune -f
  echo "-- retelechargement propre de Kafka :"
  docker compose pull -q kafka && echo "cp-kafka:7.8.0 retelechargee"
  docker compose up -d kafka
  H=$(attendre_healthy vitals-kafka 300)
  if [ "$H" = TIMEOUT ]; then
    echo "ECHEC : Kafka ne demarre toujours pas :"; docker compose logs --tail 15 kafka; exit 1
  fi
  echo "Kafka healthy apres $H s : reparation reussie"
  docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list
  docker compose stop kafka
  echo "Espace libre apres reparation : $(df -g "$HOME" | awk 'NR==2 {print $4}') Go"
  ;;
# =====================================================================
partie1)
  titre "Verification des prerequis"
  docker compose config --quiet && echo "docker compose config --quiet : compose OK (sortie vide)"
  docker images | grep -E "cp-kafka|vitals-producer|wolfi" || true
  ls -l producer/producer.py docker-compose.yml
  echo "Volume kafka-data : $(docker volume ls -q | grep -c 'kafka-data') trouve(s)"
  echo "PostgreSQL natif : $(psql --version 2>/dev/null || echo aucune)"
  [ -f MESURES_S2.md ] || printf '# Mesures — séance 2\n\n' > MESURES_S2.md
  $PY -m pip install -q -r producer/requirements.txt -r consumer/requirements.txt && echo "venv : dependances OK"

  titre "1.1 Broker seul"
  docker compose stop producer consumer >/dev/null 2>&1
  docker compose up -d kafka
  H=$(attendre_healthy vitals-kafka 120)
  if [ "$H" = TIMEOUT ]; then
    echo "ECHEC : Kafka n'est pas healthy. Dernieres lignes du conteneur :"
    docker compose logs --tail 15 kafka
    echo "=> si l'image est abimee : bash ~/s1-vitals-stream/s2.sh reparer, puis relance partie1"
    exit 1
  fi
  echo "kafka healthy apres $H s"
  kt --list
  if ! kt --list | grep -qx "$TOPIC"; then
    echo ">> topic absent : recreation a l'identique de la seance 1 (3 partitions)"
    kt --create --topic "$TOPIC" --partitions 3 --replication-factor 1
  fi
  kt --describe --topic "$TOPIC"
  AVANT=$(kt --describe --topic "$TOPIC" | grep -o 'PartitionCount: *[0-9]*' | grep -o '[0-9]*$')
  echo "MESURE 1 : partitions avant = $AVANT ; somme des offsets de fin = $(somme_fin)"

  titre "1.2 Repartition avant modification (producteur local 15 s)"
  producteur_local "$D/p1_producteur_avant.log"
  echo "-- MESURE 2 (tout le topic depuis le debut) :"
  $PY outils/repartition.py lire - "$D/repartition_avant.json"

  titre "1.3 Passage a $NB_PARTITIONS partitions"
  if [ "${AVANT:-0}" -lt "$NB_PARTITIONS" ]; then
    kt --alter --topic "$TOPIC" --partitions "$NB_PARTITIONS"
  else echo ">> deja $AVANT partitions : pas de modification"; fi
  kt --describe --topic "$TOPIC"
  $PY outils/repartition.py fin > "$D/offsets_bornes.json"
  echo "Offsets de fin releves (bornes de depart) : $(cat "$D/offsets_bornes.json")"
  producteur_local "$D/p1_producteur_apres.log"
  echo "-- MESURE 3 (uniquement les messages produits apres la modification) :"
  $PY outils/repartition.py lire "$D/offsets_bornes.json" "$D/repartition_apres.json"

  titre "1.4 Controle de fin de partie"
  kt --describe --topic "$TOPIC"
  ko --topic "$TOPIC" --time -1
  echo "Somme des offsets de fin : $(somme_fin)"
  docker compose up -d --build producer && sleep 5 && docker compose logs --tail 5 producer
  ;;
# =====================================================================
partie2)
  titre "2.2 Environnement du consommateur"
  if grep -q '^FROM cgr.dev/chainguard/wolfi-base:latest$' consumer/Dockerfile; then
    docker pull -q cgr.dev/chainguard/wolfi-base:latest
    REF=$(docker image inspect --format '{{index .RepoDigests 0}}' cgr.dev/chainguard/wolfi-base:latest)
    DIGEST=${REF#*@}
    $PY - "$DIGEST" <<'EOF'
import sys
p = "consumer/Dockerfile"
t = open(p).read().replace("FROM cgr.dev/chainguard/wolfi-base:latest\n",
                           f"FROM cgr.dev/chainguard/wolfi-base:latest@{sys.argv[1]}\n")
open(p, "w").write(t)
EOF
    echo "Base figee : $(grep '^FROM' consumer/Dockerfile)"
  fi
  $PY -m pip install -q -r consumer/requirements.txt && $PY -c "import confluent_kafka, psycopg2; print('confluent-kafka', confluent_kafka.version()[0], '| psycopg2', psycopg2.__version__)"
  docker compose up -d producer >/dev/null 2>&1
  for g in explo-earliest explo-latest explo-auto explo-manuel; do oublier_groupe $g; done

  titre "2.4 Experience 1 : groupe neuf et auto.offset.reset (MESURE 4)"
  for G in explo-earliest explo-latest; do
    R=${G#explo-}
    for n in 1 2; do
      lecteur "$G" "$R" true c1 "$D/p2_${G}_$n.log"; P=$!
      sleep 20; arreter $P
      echo "$G ($R) lancement $n : lus=$(dernier_lus "$D/p2_${G}_$n.log")  [$(grep FIN "$D/p2_${G}_$n.log")]"
    done
    if [ "$G" = explo-earliest ]; then
      echo "-- MESURE 5 : --describe explo-earliest juste apres l'arret propre"
      kg --describe --group explo-earliest
    fi
  done
  echo "-- groupes connus du broker :"; kg --list

  titre "2.5 Experience 2 : acquittement automatique contre manuel (MESURE 6)"
  for MODE in auto manuel; do
    G=explo-$MODE; AC=true; [ $MODE = manuel ] && AC=false
    lecteur "$G" earliest $AC c1 "$D/p2_${G}_avant.log"; P=$!
    sleep 15
    AV=$(dernier_lus "$D/p2_${G}_avant.log")
    kill -9 $P; wait $P 2>/dev/null
    echo "$G ($MODE) : kill -9 a $(date +%H:%M:%S) ; dernier lus= affiche avant le kill : $AV"
    kg --describe --group "$G" | tee "$D/p2_${G}_describe.txt"
    COUR=$(awk '$4 ~ /^[0-9]+$/ {s+=$4} END{print s+0}' "$D/p2_${G}_describe.txt")
    LAG=$(awk '$6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}' "$D/p2_${G}_describe.txt")
    echo "$G : CURRENT-OFFSET total = $COUR ; LAG total juste apres le kill = $LAG (lus mais non acquittes => relus)"
    T0=$(now)
    lecteur "$G" earliest $AC c1 "$D/p2_${G}_apres.log"; P=$!
    attendre "$D/p2_${G}_apres.log" ATTRIBUTION 1 90
    echo "$G : relance -> ATTRIBUTION apres $(ecart "$T0") s (expiration de la session du membre tue)"
    sleep 60; arreter $P
    echo "$G : relance -> $(grep FIN "$D/p2_${G}_apres.log")"
  done
  ;;
# =====================================================================
partie3)
  docker compose up -d producer >/dev/null 2>&1
  G=vitals-explo-groupe
  oublier_groupe $G; oublier_groupe vitals-explo-coop

  titre "3.1 Deux puis trois consommateurs (MESURES 7 et 8)"
  lecteur $G latest true cA "$D/p3_cA.log"; PA=$!
  attendre "$D/p3_cA.log" ATTRIBUTION 1 30; sleep 4
  echo "cA seul           : cA=$(attrib "$D/p3_cA.log")"
  lecteur $G latest true cB "$D/p3_cB.log"; PB=$!
  attendre "$D/p3_cB.log" ATTRIBUTION 1 30; sleep 4
  echo "cA + cB           : cA=$(attrib "$D/p3_cA.log") cB=$(attrib "$D/p3_cB.log")"
  lecteur $G latest true cC "$D/p3_cC.log"; PC=$!
  attendre "$D/p3_cC.log" ATTRIBUTION 1 30; sleep 4
  echo "cA + cB + cC      : cA=$(attrib "$D/p3_cA.log") cB=$(attrib "$D/p3_cB.log") cC=$(attrib "$D/p3_cC.log")"
  for i in A B C; do eval "L0_$i=\$(lus_attrib $D/p3_c$i.log); PARTS_$i=\"\$(attrib $D/p3_c$i.log)\""; done
  echo "-- vue du broker :"; kg --describe --group $G
  echo ">> 60 s a trois..."; sleep 60
  arreter $PA; sleep 2; arreter $PB; sleep 2; arreter $PC
  for i in A B C; do
    eval "L0=\$L0_$i; PARTS=\$PARTS_$i"
    L1=$(grep FIN "$D/p3_c$i.log" | grep -o 'lus=[0-9]*' | cut -d= -f2)
    PAT=$($PY -c "import json,sys; m=json.load(open('$D/repartition_apres.json')); ps=[s.strip() for s in sys.argv[1].strip('[]').split(',') if s.strip().isdigit()]; pats=sorted(set(x for p in ps for x in m.get(p,[]))); print(len(pats), ', '.join(pats) or '-')" "$PARTS")
    echo "MESURE 8 : c$i partitions=$PARTS lus a l'arrivee de cC=$L0 lus final=$L1 -> 60 s a trois : $((L1 - L0)) messages ; patients couverts : $PAT"
  done

  titre "3.2 Un consommateur de trop (MESURE 9)"
  N=$((NB_PARTITIONS + 1)); PIDS=""
  for i in $(seq 1 $N); do lecteur $G latest true "s$i" "$D/p3_surplus_s$i.log"; PIDS="$PIDS $!"; sleep 1; done
  echo ">> $N instances lancees ; stabilisation 20 s puis 30 s d'observation"
  sleep 20
  for i in $(seq 1 $N); do eval "LS_$i=\$(lus_attrib $D/p3_surplus_s$i.log)"; done
  sleep 30
  kg --describe --group $G
  for p in $PIDS; do kill -TERM $p 2>/dev/null; done; for p in $PIDS; do wait $p 2>/dev/null; done
  INACT=0
  for i in $(seq 1 $N); do
    F=$D/p3_surplus_s$i.log; A=$(grep ATTRIBUTION "$F" | tail -1 | sed 's/.*partitions=//')
    eval "L0=\$LS_$i"; L1=$(grep FIN "$F" | grep -o 'lus=[0-9]*' | cut -d= -f2)
    echo "s$i : derniere attribution=$A  lus pendant les 30 s=$((L1 - ${L0:-0}))  (lus total=$L1)"
    [ "$A" = "[aucune]" ] && INACT=$((INACT + 1))
  done
  echo "MESURE 9 : partitions=$NB_PARTITIONS instances=$N inactives=[aucune]:$INACT"

  titre "3.3 Perte d'un consommateur (MESURE 10)"
  lecteur $G latest true cA "$D/p3_33_cA.log"; PA=$!
  attendre "$D/p3_33_cA.log" ATTRIBUTION 1 30
  lecteur $G latest true cB "$D/p3_33_cB.log"; PB=$!
  attendre "$D/p3_33_cB.log" ATTRIBUTION 1 30; sleep 8
  echo "Stable : cA=$(attrib "$D/p3_33_cA.log") cB=$(attrib "$D/p3_33_cB.log")"
  N0=$(nb "$D/p3_33_cA.log" ATTRIBUTION); R0=$(nb "$D/p3_33_cA.log" RETRAIT)
  T0=$(now); kill -TERM $PB                      # Scenario A : Ctrl-C, close() appele
  if attendre_complet "$D/p3_33_cA.log" "$N0" $NB_PARTITIONS 90; then
    echo "MESURE 10 A (arret propre) : cA recupere tout apres $(ecart "$T0") s -> $(attrib "$D/p3_33_cA.log")"
  else echo "MESURE 10 A : pas de reattribution en 90 s"; fi
  wait $PB 2>/dev/null
  echo "MESURE 11 range : RETRAIT de cA pendant le depart de cB = $(( $(nb "$D/p3_33_cA.log" RETRAIT) - R0 )) :"
  grep RETRAIT "$D/p3_33_cA.log" | tail -n +$((R0 + 1))

  lecteur $G latest true cB "$D/p3_33_cB2.log"; PB=$!
  attendre "$D/p3_33_cB2.log" ATTRIBUTION 1 30; sleep 8
  echo "Stable : cA=$(attrib "$D/p3_33_cA.log") cB=$(attrib "$D/p3_33_cB2.log")"
  N0=$(nb "$D/p3_33_cA.log" ATTRIBUTION)
  T0=$(now); kill -STOP $PB; kill -9 $PB          # Scenario B : Ctrl-Z puis kill -9
  if attendre_complet "$D/p3_33_cA.log" "$N0" $NB_PARTITIONS 120; then
    echo "MESURE 10 B (arret brutal) : cA recupere tout apres $(ecart "$T0") s -> $(attrib "$D/p3_33_cA.log")"
  else echo "MESURE 10 B : pas de reattribution en 120 s"; fi
  wait $PB 2>/dev/null; arreter $PA

  titre "3.4 Strategie cooperative-sticky (MESURE 11)"
  GC=vitals-explo-coop
  lecteur $GC latest true cA "$D/p3_34_cA.log" cooperative-sticky; PA=$!
  attendre "$D/p3_34_cA.log" ATTRIBUTION 1 30
  lecteur $GC latest true cB "$D/p3_34_cB.log" cooperative-sticky; PB=$!
  attendre "$D/p3_34_cB.log" ATTRIBUTION 1 30; sleep 8
  echo "Lignes de cA avant le depart de cB :"; grep -E "ATTRIBUTION|RETRAIT" "$D/p3_34_cA.log"
  echo "Lignes de cB :"; grep -E "ATTRIBUTION|RETRAIT" "$D/p3_34_cB.log"
  N0=$(nb "$D/p3_34_cA.log" ATTRIBUTION); R0=$(nb "$D/p3_34_cA.log" RETRAIT)
  T0=$(now); kill -TERM $PB
  attendre "$D/p3_34_cA.log" ATTRIBUTION $((N0 + 1)) 60
  echo "Reattribution a cA apres $(ecart "$T0") s : $(grep ATTRIBUTION "$D/p3_34_cA.log" | tail -1)"
  sleep 3; wait $PB 2>/dev/null
  echo "MESURE 11 cooperative-sticky : RETRAIT de cA pendant le depart de cB = $(( $(nb "$D/p3_34_cA.log" RETRAIT) - R0 )) :"
  grep RETRAIT "$D/p3_34_cA.log" | tail -n +$((R0 + 1))
  arreter $PA
  ;;
# =====================================================================
partie4)
  titre "4.2 PostgreSQL (MESURE 12)"
  docker compose config --quiet && echo "compose OK"
  docker compose pull -q postgres
  T0=$(now); docker compose up -d postgres
  echo "MESURE 12 : delai up -d postgres -> healthy = $(attendre_healthy vitals-postgres 120) s"
  docker compose ps postgres
  psql_ -c '\dt'
  psql_ -c '\d vitals_raw'
  psql_ -c '\dv'

  titre "4.3 Consommateur depuis l'hote, 60 s (MESURE 13)"
  docker compose up -d producer >/dev/null 2>&1
  KAFKA_BOOTSTRAP=localhost:29092 PG_HOST=localhost PG_PORT=55432 CONSUMER_GROUP=vitals-persister \
  AUTO_OFFSET_RESET=earliest $PY consumer/consumer.py > "$D/p4_consumer_hote.log" 2>&1 &
  P=$!; sleep 60; arreter $P
  head -3 "$D/p4_consumer_hote.log"; echo "..."; tail -4 "$D/p4_consumer_hote.log"
  psql_ -c 'SELECT * FROM v_vitals_controle;'
  psql_ -c 'SELECT kafka_partition, COUNT(*), MIN(kafka_offset), MAX(kafka_offset) FROM vitals_raw GROUP BY 1 ORDER BY 1;'

  titre "4.4 Chaine complete dans le Compose (MESURE 14)"
  docker compose up -d --build
  sleep 25
  docker compose ps
  echo "Services healthy : $(docker compose ps --format '{{.Status}}' | grep -c '(healthy)')"
  docker compose logs --tail 20 consumer
  L0=$(lignes_totales); sleep 60; L1=$(lignes_totales)
  echo "lignes_totales : $L0 -> $L1 en 60 s, soit +$((L1 - L0)) lignes"
  docker compose logs --since 60s consumer | grep -o 'LOT taille=[0-9]*' | cut -d= -f2 \
    | awk '{s+=$1; n++} END{if(n) printf "Taille moyenne des lots sur 60 s : %.1f (sur %d lots)\n", s/n, n}'
  ;;
# =====================================================================
partie4b)
  # Reprise de 4.4 apres correction du Dockerfile (droits de lecture du code).
  titre "Mesure 12 retrouvee dans les journaux de PostgreSQL"
  DEBUT=$(docker inspect -f '{{.State.StartedAt}}' vitals-postgres)
  PRET=$(docker logs -t vitals-postgres 2>&1 | grep 'ready to accept connections' | tail -1 | awk '{print $1}')
  echo "Conteneur demarre : $DEBUT"
  echo "Derniere ligne 'ready to accept connections' : $PRET"
  $PY - "$DEBUT" "$PRET" <<'EOF2'
import sys, datetime as dt
def t(s):
    s = s.rstrip("Z")
    base, _, frac = s.partition(".")
    return dt.datetime.fromisoformat(base) + dt.timedelta(seconds=float("0." + (frac or "0")))
d = (t(sys.argv[2]) - t(sys.argv[1])).total_seconds()
print(f"MESURE 12 : serveur pret {d:.1f} s apres le demarrage ; healthy au premier test suivant "
      f"(interval 5s, start_period 10s) -> environ {max(5, 5 * -(-d // 5)):.0f} s")
EOF2
  docker compose ps postgres

  titre "4.4 Chaine complete dans le Compose (MESURE 14)"
  docker compose up -d --build consumer
  sleep 25
  docker compose ps
  echo "Services healthy : $(docker compose ps --format '{{.Status}}' | grep -c '(healthy)')"
  docker compose logs --tail 20 consumer
  L0=$(lignes_totales); sleep 60; L1=$(lignes_totales)
  echo "lignes_totales : $L0 -> $L1 en 60 s, soit +$((L1 - L0)) lignes"
  docker compose logs --since 60s consumer | grep -o 'LOT taille=[0-9]*' | cut -d= -f2 \
    | awk '{s+=$1; n++} END{if(n) printf "Taille moyenne des lots sur 60 s : %.1f (sur %d lots)\n", s/n, n}'
  ;;
# =====================================================================
partie5)
  G=vitals-persister
  docker compose up -d >/dev/null 2>&1; sleep 10
  titre "5.1 Lag en regime nominal (MESURE 15)"
  for t in 0 30 60; do
    [ $t -gt 0 ] && sleep 30
    echo "t0+${t}s : somme des LAG = $(somme_lag $G)"
  done
  kg --describe --group $G

  titre "5.2 Faire decrocher le consommateur (MESURE 16)"
  docker compose config | grep -E "INTERVAL_S|BATCH_SIZE"
  export INTERVAL_S=0.005 BATCH_SIZE=1    # 12 msg / 0,005 s = 2 400 msg/s theoriques
  docker compose up -d producer consumer
  docker compose exec -T producer env | grep INTERVAL_S
  docker update --cpus 0.05 $(docker compose ps -q consumer) >/dev/null && echo "consommateur bride a 0,05 CPU"
  sleep 5; F0=$(somme_fin); TF=$(now)
  PREC=""
  for t in 0 30 60 90 120; do
    [ $t -gt 0 ] && sleep 30
    L=$(somme_lag $G)
    echo "t0+${t}s : somme des LAG = $L ${PREC:+(ecart +$((L - PREC)))}"; PREC=$L
  done
  echo "Debit producteur mesure : $(awk -v a="$F0" -v b="$(somme_fin)" -v t="$(ecart "$TF")" 'BEGIN{printf "%.0f", (b-a)/t}') msg/s"

  titre "Passage a deux consommateurs (MESURE 17)"
  echo "Somme des LAG juste avant le --scale : $(somme_lag $G)"
  docker compose up -d --scale consumer=2
  docker update --cpus 0.05 $(docker compose ps -q consumer) >/dev/null && echo "les 2 consommateurs bridés a 0,05 CPU"
  sleep 15; kg --describe --group $G
  sleep 45
  echo "Somme des LAG 60 s apres : $(somme_lag $G) ; membres du groupe : $(membres $G)"
  kg --describe --group $G

  titre "Retour a l'etat nominal"
  unset INTERVAL_S BATCH_SIZE
  docker compose up -d --scale consumer=1
  docker compose exec -T producer env | grep INTERVAL_S
  docker inspect --format '{{.HostConfig.NanoCpus}}' $(docker compose ps -q consumer)
  echo ">> rattrapage du retard accumule..."
  for i in $(seq 60); do
    L=$(somme_lag $G); echo "$(date +%H:%M:%S) lag=$L"
    [ "$L" -lt 300 ] && break; sleep 15
  done

  titre "5.3 Stockage indisponible (MESURE 18)"
  psql_ -c 'SELECT * FROM v_vitals_controle;'
  A0=$(lignes_totales); echo "lignes_totales avant la coupure : $A0"
  CID=$(docker compose ps -q consumer)
  docker compose stop postgres; echo ">> postgres arrete a $(date +%H:%M:%S)"
  for t in 15 30 45 60 75 90; do
    sleep 15
    echo "+${t}s : consommateur $(docker inspect -f '{{.State.Status}} redemarrages={{.RestartCount}}' "$CID") ; lag=$(somme_lag $G)"
  done
  echo "-- logs du consommateur pendant la coupure :"
  docker compose logs --since 100s consumer | tail -30
  echo "Premiere ligne d'erreur : $(docker compose logs --since 100s consumer | grep -m1 ERREUR)"
  echo "Somme des LAG apres 90 s de coupure : $(somme_lag $G)"
  docker compose start postgres; echo ">> postgres relance a $(date +%H:%M:%S)"
  sleep 60
  echo "lignes_totales 60 s apres le redemarrage : $(lignes_totales) (avant : $A0)"
  echo "Somme des LAG 60 s apres le redemarrage : $(somme_lag $G)"
  echo "Consommateur : $(docker inspect -f '{{.State.Status}} redemarrages={{.RestartCount}}' "$CID")"
  docker compose logs --since 70s consumer | tail -8

  titre "5.4 Reprise sans perte ni doublon (MESURE 19)"
  psql_ -c 'SELECT * FROM v_vitals_controle;'
  echo "lignes_totales avant le SIGKILL : $(lignes_totales)"
  docker compose kill -s SIGKILL consumer; echo ">> SIGKILL a $(date +%H:%M:%S)"
  kg --describe --group $G
  docker compose up -d consumer
  echo ">> 90 s (dont ~45 s d'expiration de session du membre tue)..."; sleep 90
  docker compose stop consumer && docker compose logs --tail 1 consumer && docker compose start consumer
  echo "-- doublons Kafka (0 ligne attendue) :"
  psql_ -c 'SELECT kafka_partition, kafka_offset, COUNT(*) FROM vitals_raw GROUP BY 1,2 HAVING COUNT(*) > 1;'
  echo "-- trous d'offsets (lignes = etendue attendu) :"
  psql_ -c 'SELECT kafka_partition, COUNT(*) AS lignes, MAX(kafka_offset) - MIN(kafka_offset) + 1 AS etendue FROM vitals_raw GROUP BY 1 ORDER BY 1;'
  psql_ -c 'SELECT * FROM v_vitals_controle;'
  echo "-- bonus extension 2 : latence de bout en bout (ingested_at - event_time), hors rattrapages :"
  psql_ -c "SELECT percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ingested_at - event_time)) AS p50_p95_p99_s FROM vitals_raw WHERE ingested_at > now() - interval '60 seconds';"
  ;;
# =====================================================================
nettoyage)
  titre "Sauvegarde du schema puis destruction"
  docker compose exec -T postgres pg_dump -U hs_user -d healthstream --schema-only > sql/schema_dump.sql \
    && echo "schema sauvegarde : $(wc -l < sql/schema_dump.sql) lignes"
  docker compose down -v
  docker compose ps -a
  echo "Volumes restants : $(docker volume ls | grep -E 'vitals|kafka-data|pg-data' || echo aucun)"
  docker compose up -d kafka && sleep 25 && echo "-- groupes apres down -v :" && kg --list && docker compose down -v
  docker system df

  titre "Archive du livrable"
  ARCHIVE=S2_KORICH_Marwan.zip
  ( cd .. && rm -f "s1-vitals-stream/$ARCHIVE" && zip -rq "s1-vitals-stream/$ARCHIVE" s1-vitals-stream \
      -x 's1-vitals-stream/.venv/*' 's1-vitals-stream/sql/schema_dump.sql' '*/__pycache__/*' \
         '*.pyc' '*/.DS_Store' "s1-vitals-stream/$ARCHIVE" )
  unzip -l "$ARCHIVE" | tail -n +4 | awk '{print $4}' | grep -v '^$' | head -40
  echo ">> Archive : ~/s1-vitals-stream/$ARCHIVE"
  ;;
*) echo "Partie inconnue : $PARTIE"; exit 1 ;;
esac
echo; echo ">> FIN $PARTIE ($(date +%H:%M:%S))"

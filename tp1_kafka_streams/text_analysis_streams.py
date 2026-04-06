"""
Exercice 1 : Analyse de Données Texte - Kafka Streams (Python)
Big Data 2026 - Mr. Abdelmajid BOUSSELHAM

Architecture :
  text-input  -->  [Nettoyage + Filtrage]  -->  text-clean
                                            -->  text-dead-letter
"""

from confluent_kafka import Consumer, Producer, KafkaError
import re
import logging
import signal
import sys

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC_INPUT       = "text-input"
TOPIC_CLEAN       = "text-clean"
TOPIC_DEAD_LETTER = "text-dead-letter"

FORBIDDEN_WORDS   = {"HACK", "SPAM", "XXX"}
MAX_LENGTH        = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Étape 3 : Nettoyage du texte
# ─────────────────────────────────────────────
def clean_text(text: str) -> str:
    """
    - Supprime les espaces avant/après (trim)
    - Remplace les espaces multiples par un seul espace
    - Convertit en majuscules
    """
    text = text.strip()
    text = re.sub(r" +", " ", text)
    text = text.upper()
    return text


# ─────────────────────────────────────────────
# Étape 4 : Validation / Filtrage
# ─────────────────────────────────────────────
def validate(text: str) -> tuple[bool, str]:
    """
    Retourne (is_valid, reason).
    - Rejette les messages vides
    - Rejette les messages contenant des mots interdits
    - Rejette les messages > 100 caractères
    """
    if not text:
        return False, "Message vide ou uniquement des espaces"

    words_in_text = set(re.findall(r"\b\w+\b", text))
    found = words_in_text & FORBIDDEN_WORDS
    if found:
        return False, f"Mot(s) interdit(s) détecté(s) : {', '.join(found)}"

    if len(text) > MAX_LENGTH:
        return False, f"Message trop long ({len(text)} > {MAX_LENGTH} caractères)"

    return True, "OK"


# ─────────────────────────────────────────────
# Callback de livraison Producer
# ─────────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        log.error("❌ Échec d'envoi vers %s : %s", msg.topic(), err)
    else:
        log.info("✅ Envoyé vers [%s] : %s", msg.topic(), msg.value().decode())


# ─────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────
def run_pipeline():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id":          "text-analysis-group",
        "auto.offset.reset": "earliest",
    })

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    })

    consumer.subscribe([TOPIC_INPUT])
    log.info("🚀 Pipeline démarré. Écoute sur le topic : %s", TOPIC_INPUT)

    # Arrêt propre sur Ctrl+C
    running = True
    def shutdown(sig, frame):
        nonlocal running
        log.info("⏹  Arrêt du pipeline...")
        running = False
    signal.signal(signal.SIGINT, shutdown)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Erreur Kafka : %s", msg.error())
                continue

            raw = msg.value().decode("utf-8")
            log.info("📥 Reçu  : %r", raw)

            # ── Étape 3 : Nettoyage ──────────────────
            cleaned = clean_text(raw)

            # ── Étape 4 : Validation ─────────────────
            is_valid, reason = validate(cleaned)

            # ── Étape 5 : Routage ────────────────────
            if is_valid:
                log.info("   ✔ Valide  → [%s] : %r", TOPIC_CLEAN, cleaned)
                producer.produce(
                    TOPIC_CLEAN,
                    value=cleaned.encode("utf-8"),
                    callback=delivery_report,
                )
            else:
                log.warning("   ✘ Invalide (%s) → [%s] : %r", reason, TOPIC_DEAD_LETTER, raw)
                producer.produce(
                    TOPIC_DEAD_LETTER,
                    value=raw.encode("utf-8"),      # message original, non modifié
                    callback=delivery_report,
                )

            producer.poll(0)   # déclenche les callbacks sans bloquer

    finally:
        producer.flush()
        consumer.close()
        log.info("Pipeline arrêté proprement.")


if __name__ == "__main__":
    run_pipeline()

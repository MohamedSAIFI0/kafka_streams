"""
verify_results.py  –  Vérification des résultats (Étape 6 - suite)
                      Lit text-clean et text-dead-letter simultanément
Big Data 2026 - Mr. Abdelmajid BOUSSELHAM
"""

from confluent_kafka import Consumer, KafkaError
import signal

BOOTSTRAP_SERVERS  = "localhost:9092"
OUTPUT_TOPICS      = ["text-clean", "text-dead-letter"]
READ_TIMEOUT_SEC   = 5      # arrêt si aucun message pendant N secondes


def verify():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id":          "verifier-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(OUTPUT_TOPICS)

    counts = {"text-clean": 0, "text-dead-letter": 0}
    idle   = 0

    running = True
    def shutdown(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, shutdown)

    print("\n🔍 Vérification des topics de sortie\n")
    print(f"{'TOPIC':<22} {'MESSAGE'}")
    print("─" * 70)

    while running:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            idle += 1
            if idle >= READ_TIMEOUT_SEC:
                print(f"\n⏱  Aucun nouveau message depuis {READ_TIMEOUT_SEC}s. Arrêt.")
                break
            continue

        idle = 0   # reset du compteur d'inactivité

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Erreur : {msg.error()}")
            continue

        topic   = msg.topic()
        value   = msg.value().decode("utf-8")
        counts[topic] += 1

        icon = "✅" if topic == "text-clean" else "❌"
        print(f"{icon} {topic:<20} {value!r}")

    consumer.close()

    # ── Résumé ────────────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print("📊 Résumé :")
    print(f"   ✅ text-clean        : {counts['text-clean']} message(s)")
    print(f"   ❌ text-dead-letter  : {counts['text-dead-letter']} message(s)")
    total = sum(counts.values())
    print(f"   📨 Total traité      : {total} message(s)\n")


if __name__ == "__main__":
    verify()

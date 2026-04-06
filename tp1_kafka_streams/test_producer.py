"""
test_producer.py  –  Envoi de messages de test dans text-input (Étape 6)
Big Data 2026 - Mr. Abdelmajid BOUSSELHAM
"""

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_INPUT = "text-input"

# ─── Jeu de messages de test ──────────────────────────────────────────────────
# Format : (message, commentaire attendu)
TEST_MESSAGES = [
    # ✅ Valides
    ("  Bonjour tout le monde  ",        "valide – trim + majuscules"),
    ("hello   kafka   streams",          "valide – espaces multiples"),
    ("Big Data 2026",                    "valide – message normal"),
    ("python   est   super",             "valide – espaces multiples"),
    ("Analyse de   données texte",       "valide – nettoyage complet"),

    # ❌ Invalides
    ("   ",                              "invalide – vide / espaces seuls"),
    ("",                                 "invalide – chaîne vide"),
    ("Ceci contient du SPAM dans le texte",  "invalide – mot interdit SPAM"),
    ("Tentative de HACK du système",         "invalide – mot interdit HACK"),
    ("Contenu XXX non autorisé",             "invalide – mot interdit XXX"),
    ("A" * 101,                              "invalide – dépasse 100 caractères"),
]


def delivery_report(err, msg):
    status = "✅ Envoyé" if not err else f"❌ Erreur : {err}"
    print(f"  {status}  →  {msg.value().decode()[:60]!r}")


def send_test_messages():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"\n📤 Envoi de {len(TEST_MESSAGES)} messages dans [{TOPIC_INPUT}]\n")
    print(f"{'MESSAGE':<45} {'COMMENTAIRE'}")
    print("─" * 80)

    for message, comment in TEST_MESSAGES:
        display = repr(message[:42]) if len(message) > 42 else repr(message)
        print(f"{display:<45} {comment}")
        producer.produce(
            TOPIC_INPUT,
            value=message.encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

    producer.flush()
    print("\n✔ Tous les messages ont été envoyés.")
    print("▶ Lancez maintenant : python text_analysis_streams.py\n")


if __name__ == "__main__":
    send_test_messages()

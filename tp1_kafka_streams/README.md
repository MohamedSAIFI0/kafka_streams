# Exercice 1 : Analyse de Données Texte avec Kafka (Python)
**Big Data 2026 – Mr. Abdelmajid BOUSSELHAM**

---

## 📁 Fichiers

| Fichier                     | Rôle                                              |
|-----------------------------|---------------------------------------------------|
| `setup_topics.py`           | Crée les 3 topics Kafka (Étape 1)                 |
| `text_analysis_streams.py`  | Pipeline principal : nettoyage + filtrage + routage |
| `test_producer.py`          | Envoie des messages de test dans `text-input`     |
| `verify_results.py`         | Lit et affiche le contenu des topics de sortie    |

---

## ⚙️ Prérequis

```bash
pip install confluent-kafka
```

Kafka doit tourner sur `localhost:9092`.  
Pour démarrer Kafka rapidement avec Docker :

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ENABLE_KRAFT=yes \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:latest
```

---

## 🚀 Ordre d'exécution

### Étape 1 – Créer les topics
```bash
python setup_topics.py
```
Crée :
- `text-input`
- `text-clean`
- `text-dead-letter`

---

### Étape 2 – Lancer le pipeline (dans un terminal dédié)
```bash
python text_analysis_streams.py
```
Le pipeline écoute `text-input` en continu et route les messages.

---

### Étape 3 – Envoyer les messages de test (autre terminal)
```bash
python test_producer.py
```
Envoie 11 messages (5 valides, 6 invalides).

---

### Étape 4 – Vérifier les résultats
```bash
python verify_results.py
```

---

## 🔄 Architecture du Pipeline

```
text-input
    │
    ▼
[1] clean_text()
    • strip()               → supprime espaces avant/après
    • re.sub(r" +", " ", …) → remplace espaces multiples
    • upper()               → convertit en majuscules
    │
    ▼
[2] validate()
    • vide ?            → ❌ dead-letter
    • mot interdit ?    → ❌ dead-letter  (HACK, SPAM, XXX)
    • len > 100 ?       → ❌ dead-letter
    │
    ├─ valide   → text-clean
    └─ invalide → text-dead-letter (message ORIGINAL non modifié)
```

---

## 📋 Règles de filtrage

| Règle                         | Exemple                             | Résultat     |
|-------------------------------|-------------------------------------|--------------|
| Message vide / blancs seuls   | `"   "`                             | dead-letter  |
| Mot interdit `SPAM`           | `"Ceci est du SPAM"`                | dead-letter  |
| Mot interdit `HACK`           | `"Tentative de HACK"`               | dead-letter  |
| Mot interdit `XXX`            | `"Contenu XXX"`                     | dead-letter  |
| Longueur > 100                | `"A" * 101`                         | dead-letter  |
| Message normal                | `"  hello   kafka  "`               | → `"HELLO KAFKA"` dans text-clean |

---

## 📊 Résultats attendus avec test_producer.py

**text-clean (5 messages) :**
```
"BONJOUR TOUT LE MONDE"
"HELLO KAFKA STREAMS"
"BIG DATA 2026"
"PYTHON EST SUPER"
"ANALYSE DE DONNÉES TEXTE"
```

**text-dead-letter (6 messages, originaux) :**
```
"   "
""
"Ceci contient du SPAM dans le texte"
"Tentative de HACK du système"
"Contenu XXX non autorisé"
"AAA...AAA"  (101 caractères)
```

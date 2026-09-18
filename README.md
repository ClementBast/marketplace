# Projet Marketplace Analytics — MVP plateforme data

Pipeline ELT pour la marketplace fictive **Maelys** : ingestion API → raw layer MinIO →
Data Warehouse PostgreSQL (schéma en étoile) → dashboards Metabase.

**Adaptation par rapport au cahier des charges** : Airflow (non vu en cours) est
remplacé par **n8n** (vu au TP6). Les "DAGs" deviennent des workflows n8n planifiés,
le "Custom Hook" devient un credential Header Auth + nœuds HTTP Request, les macros
`{{ ds }}` deviennent le nœud "Date cible".

## Architecture

```mermaid
flowchart LR
    subgraph src["Source"]
        api[("API Marketplace<br/>Flask simulée")]
    end

    subgraph orch["Orchestration — n8n"]
        wf1["wf1 dims_refresh_daily"]
        wf2["wf2 orders_ingest_daily"]
        wf4["wf4 analytics_aggregate<br/>(appelé par wf2)"]
        wf3["wf3 anomaly_detect_daily<br/>(bonus)"]
    end

    subgraph store["Stockage"]
        minio[("MinIO<br/>data-lake raw/orders/dt=")]
        pg[("PostgreSQL<br/>staging / dwh / analytics")]
    end

    subgraph bi["Serving"]
        mb["Metabase<br/>dashboards"]
    end

    api --> wf1
    api --> wf2
    wf2 -- "JSON brut" --> minio
    wf1 -- "upsert dims" --> pg
    wf2 -- "staging → dwh" --> pg
    wf2 --> wf4
    wf4 -- "analytics" --> pg
    wf3 --> pg
    pg --> mb
```

## Structure

```
marketplace/
├── docker-compose.yml      # stack complète
├── api/
│   ├── app.py              # API Flask simulée (déterministe, Bearer auth)
│   ├── Dockerfile
│   └── requirements.txt
├── init-db/
│   └── 01_schema.sql       # staging / dwh / analytics (exécuté au 1er démarrage)
├── n8n/
│   ├── wf1_dims_refresh_daily.json    # upsert dim_seller + dim_product
│   ├── wf2_orders_ingest_daily.json   # pipeline principal extract→raw→staging→dwh
│   ├── wf3_anomaly_detect_daily.json  # bonus : alerte CA < 70% moyenne 7j + detection fraude
│   └── wf4_analytics_aggregate.json   # agrégats analytics (appelé par wf2)
├── scripts/
│   ├── verifier.sh         # vérifs services + idempotence
│   └── setup_metabase.py   # setup Metabase auto (compte, DWH, dashboards)
└── tests/
    ├── test_api.py         # tests pytest sur l'API (7 tests)
    └── test_workflows.py   # tests pytest sur les workflows n8n (6 tests)
```

## Lancement

```bash
cd marketplace
cp .env.example .env      # credentials Postgres/MinIO (valeurs de demo)
docker compose up -d --build
docker compose ps        # attendre que tout soit "Up" (~60s pour metabase)
```

> Les secrets ne sont plus en dur dans `docker-compose.yml` : ils sont lus
> depuis `.env` (gitignoré). `.env.example` sert de template.

| Service | URL | Credentials |
|---------|-----|-------------|
| API Marketplace | http://localhost:5000 | Bearer `formation-token-2026` |
| n8n | http://localhost:5678 | compte owner (créé au 1er accès) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Metabase | http://localhost:3000 | à créer (admin@maelys.local) |
| PostgreSQL DWH | localhost:5433 | dwh_user / dwh_password, db `dwh` |

Tester l'API à la main :

```bash
curl http://localhost:5000/health
curl -H "Authorization: Bearer formation-token-2026" \
     "http://localhost:5000/orders?date=2026-04-07"
# Sans le header -> 401 (volontaire : le token va dans un credential, pas dans le code)
```

## Configuration n8n (une seule fois)

1. Ouvrir http://localhost:5678, créer le compte owner.
2. Créer **3 credentials** (menu Credentials → Add credential) :
   - **Header Auth** nommé `Marketplace API` : Name = `Authorization`,
     Value = `Bearer formation-token-2026`
   - **Postgres** nommé `Postgres DWH` : Host = `postgres-dwh`, Port = `5432`,
     Database = `dwh`, User = `dwh_user`, Password = `dwh_password`
   - **S3** nommé `MinIO S3` : Endpoint = `http://minio:9000`,
     Access Key = `minioadmin`, Secret = `minioadmin123`, Region = `us-east-1`
     > Le test du credential S3 peut échouer ("Forbidden") — sauvegarder quand
     > même, il teste contre AWS STS, pas MinIO (cf. TP6).
3. Importer les workflows : menu → Import from File → les 4 JSON de `n8n/`
   (un import par workflow : importer, sauvegarder, puis nouveau workflow).
4. Sur chaque nœud, re-sélectionner le credential correspondant (les IDs ne
   survivent pas à l'import), puis sauvegarder.
5. Dans **wf2**, sur le nœud `Agreger analytics` : sélectionner le workflow
   `marketplace_analytics_aggregate_daily` (wf4) dans la liste déroulante.
6. Activer les workflows (toggle "Active") pour le schedule quotidien.

## Exécution

**Ordre important** : lancer **wf1** (dimensions) avant **wf2** (commandes) la
première fois — `fact_orders` a des FK vers `dim_seller` / `dim_product`.

Dans n8n : ouvrir le workflow → **Execute Workflow**.

Ce que fait wf2 :

```
Schedule → Date cible (= {{ ds }})
  → GET /orders?date=...           (Header Auth, ~8 500 lignes)
  → Preparer                       (construit les requêtes SQL, chunks de 3 000)
  → Upload raw MinIO               (data-lake/raw/orders/dt=YYYY-MM-DD/orders.json)
  → DELETE staging.orders WHERE dt (purge partition)
  → Batcher staging                (émet 1 item par chunk d'INSERT)
  → INSERT staging.orders          (s'exécute une fois par chunk)
  → Fin staging                    (re-compacte en 1 item — évite de rejouer les faits)
  → DELETE dwh.fact_orders WHERE dt
  → INSERT dwh.fact_orders         (transform, jointures dims)
  → Execute Workflow → wf4         (UPSERT daily_summary / seller_daily / category_daily)
  → INSERT file_ingestion_log
```

> **Batching** : à ~8 500 commandes/jour, un seul `INSERT` géant serait trop lourd
> (taille de requête). `Preparer` découpe en chunks, `Batcher staging` émet un
> item par chunk → le nœud Postgres s'exécute une fois par chunk.
> `Fin staging` re-compacte en **1 item** : sans lui, les nœuds suivants se
> ré-exécuteraient une fois par chunk et `INSERT fact_orders` tournerait 3 fois
> → triplons malgré le DELETE+INSERT.

wf4 est découplé de wf2 (déclenché par `Execute Workflow Trigger`) : c'est
l'équivalent du DAG `marketplace_analytics_aggregate_daily` "asset-scheduled"
du cahier des charges — il ne tourne que si le load des faits a réussi.

## Backfill — rejouer des dates passées (US-06)

L'API est déterministe (`seed = md5(date)`), donc on peut recharger
n'importe quelle date. Procédure pour un backfill manuel sur 7 jours :

1. Dans **wf2**, ouvrir le nœud **Date cible** et remplacer l'expression
   `{{ $now.toFormat('yyyy-MM-dd') }}` par une date fixe, ex. `2026-04-01`.
2. **Execute Workflow** → la partition `dt=2026-04-01` est rechargée
   (raw MinIO + staging + fact_orders + analytics).
3. Répéter pour `2026-04-02` … `2026-04-07`.
4. Vérifier :

```bash
docker exec mp_postgres_dwh psql -U dwh_user -d dwh \
  -c "SELECT dt, COUNT(*) FROM dwh.fact_orders GROUP BY dt ORDER BY dt;"
# → une ligne par date backfillée, COUNT stable d'un run à l'autre
```

Le DELETE + INSERT par partition garantit que rejouer une date déjà chargée
ne crée pas de doublons — c'est la propriété d'idempotence.

## Vérification de l'idempotence

```bash
bash scripts/verifier.sh            # affiche le COUNT de fact_orders
# exécuter wf2 dans n8n, puis :
bash scripts/verifier.sh 2026-04-07 # même COUNT, pas 2x
```

Ou à la main :

```bash
docker exec mp_postgres_dwh psql -U dwh_user -d dwh \
  -c "SELECT COUNT(*) FROM dwh.fact_orders WHERE dt='2026-04-07';"
```

## Tests pytest

```bash
pip install pytest requests
pytest tests/ -v        # 13 tests
```

- `test_api.py` (7 tests, API requise) : health, 401, 400, déterminisme,
  schéma des commandes, cohérence des FK, paramètre limit
- `test_workflows.py` (6 tests, aucun service requis) : workflows JSON
  valides, trigger présent, pattern DELETE+INSERT idempotent, partition
  `dt=` MinIO, agrégation découplée en sous-workflow, pas de token en dur —
  l'équivalent des tests DagBag du cahier des charges

## Metabase

Setup automatique (compte admin + connexion DWH + 3 dashboards) :

```bash
python scripts/setup_metabase.py
# -> http://localhost:3000  (admin@maelys.local / Admin2026!)
```

Dashboards créés :
- **Executive Summary** : CA du jour (KPI), courbe CA 30j, top 5 vendeurs
- **Top Sellers** : top 10 vendeurs du mois, évolution CA top 3, vendeurs
  inactifs > 7 jours
- **Finance & Catalogue** (bonus) : commissions par jour, CA par catégorie
- **Fraude potentielle** : nb de commandes à prix suspect (écart > 15 % vs
  catalogue), top 50 des écarts de prix, vendeurs à fort taux d'annulation
  (> 25 % vs ~5 % en moyenne). L'API injecte ~2 % de commandes à prix
  anormal et 5 vendeurs "fraudeurs" (~60 % d'annulations) de façon
  déterministe — le dashboard les détecte réellement.

Pour le faire à la main à la place : connexion PostgreSQL, host
`postgres-dwh` (nom Docker, pas localhost), port **interne** `5432`,
db `dwh`, `dwh_user` / `dwh_password`, puis requêter `analytics.*`.

## Modèle de données

```mermaid
erDiagram
    dim_date ||--o{ fact_orders : dt
    dim_seller ||--o{ fact_orders : seller_id
    dim_product ||--o{ fact_orders : product_id
    dim_seller ||--o{ dim_product : seller_id

    dim_seller { text seller_id PK  string name  string country  string city }
    dim_product { text product_id PK  string name  string category  numeric price }
    dim_date { date dt PK  int year  int month  int day_of_week }
    fact_orders { text order_id PK  date dt FK  text seller_id FK  text product_id FK  numeric total  numeric commission }
```

- `staging.orders` : brut typé, purgé puis rechargé par partition `dt`
- `analytics.daily_summary` / `seller_daily` / `category_daily` : agrégats
  pré-calculés (UPSERT → rejouable)
- Les commandes `cancelled` sont dans `fact_orders` mais exclues des agrégats CA

## Choix techniques justifiés

- **n8n au lieu d'Airflow** : vu en cours (TP6), suffisant pour 3 workflows
  quotidiens, zéro code d'infra. Limites connues : pas de notion de "DAG run" ni
  de backfill natif → contourné par le nœud "Date cible" éditable.
- **DELETE + INSERT** pour `staging → dwh` : simple, performant en batch,
  état connu après chaque run. Les tables `analytics` utilisent `UPSERT` sur leur
  PK — équivalent idempotent en une seule requête par nœud (le nœud Postgres
  n8n n'accepte qu'un statement par exécution).
- **MinIO en raw layer** : le JSON brut est rejouable si la transform a un bug,
  sans re-solliciter l'API (pattern data lake first, cf. TP5/TP6).
- **API déterministe** (`seed = md5(date)`) : permet de prouver l'idempotence.
- **Volumétrie réelle du cahier des charges** : 2 400 vendeurs, 180 000
  produits, ~8 500 commandes/jour, CA ~140 k€/jour ≈ 4,2 M€/mois
  (panier moyen ~17 €). wf1 insère les produits par chunks de 2 000 (~90
  requêtes) et wf2 le staging par chunks de 3 000 : à ce volume, un INSERT
  unique dépasserait les limites raisonnables de taille de requête.
- **API déterministe** (`seed = md5(date)`) : permet de prouver l'idempotence
  — vérifié : 2 runs wf2 sur la même date → `COUNT(*)` stable à 8 933.
- **Fraude simulée** : l'API injecte ~1,5 % de prix bradés (x0,4-0,75), ~0,5 %
  de prix gonflés (x1,6-2,5) et 5 vendeurs à ~60 % d'annulations. Détectable
  via `fact_orders.unit_price` vs `dim_product.price` et le taux de
  `cancelled` par vendeur — cf. dashboard "Fraude potentielle".
- **Détection fraude automatique** (wf3, nœud `Detecter fraude`) : chaque jour,
  insère dans `analytics.alerts` les vendeurs à > 25 % d'annulations (min. 3
  commandes) et ceux ayant des commandes à > 15 % d'écart avec le prix
  catalogue (`FRAUD_CANCEL` / `FRAUD_PRICE`). L'INSERT est idempotent
  (`NOT EXISTS`) — rejouer wf3 ne duplique pas les alertes.

## Pièges connus (retours TP6 + énoncé)

| Piège | Solution |
|-------|----------|
| Test credential S3 "Forbidden" | Normal (test AWS STS) → sauvegarder quand même |
| S3 "connection cannot be established" | Activer **Force Path Style** dans le credential S3 (sinon n8n tape `bucket.minio` en virtual-host) |
| wf4 : "Failed query: undefined" | Le trigger `executeWorkflowTrigger` doit être en mode **"Accept all input data"** — et ne jamais lancer wf4 à la main (il reçoit ses requêtes de wf2) |
| Metabase "cannot connect" | Host = `postgres-dwh`, port **interne** `5432` |
| Dashboards perdus au `down -v` | Volume `metabase-data` (déjà dans le compose) |
| Port 9000 occupé (ClickHouse tp1) | Remapper MinIO : `9010:9000` / `9011:9001` |
| FK `fact_orders` en erreur | Lancer wf1 (dims) avant wf2 |
| Bucket `data-lake` absent | `docker compose restart minio-init` ou le créer via la console MinIO |

## Commandes utiles

```bash
docker compose up -d --build      # démarrer
docker compose logs -f n8n        # logs d'un service
docker compose down               # stop (données conservées)
docker compose down -v            # reset complet (données perdues)
docker exec mp_postgres_dwh psql -U dwh_user -d dwh -c "\dt dwh.*"
```

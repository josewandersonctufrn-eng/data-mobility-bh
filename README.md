# Data Mobility BH — Medallion Pipeline (Bronze / Silver / Gold)

Este repositório implementa (em código) a solução descrita no PDF **“Pipeline de Dados: Mobilidade Urbana BH (Arquitetura Medallion)”**: ingestão via CKAN (PBH) para **Bronze (Parquet)**, tratamento e deduplicação na **Silver (Delta Lake)** e geração de agregações e fatos na **Gold (Delta Lake / Tabelas analíticas)**.

## Visão geral (Problema → Solução)
**Problema:** dados de GPS de ônibus chegam com alta frequência, podem conter duplicidades (mesmo `id_veiculo` + timestamp), nulos, outliers de velocidade e coordenadas fora da malha (off-grid). Isso degrada métricas (velocidade média, pontualidade) e inviabiliza governança.

**Solução:** arquitetura Medallion com idempotência e governança:
- **Bronze (Raw / Imutável):** persiste payload da API em Parquet particionado por data de ingestão.
- **Silver (Trusted):** tipagem, filtros (GPS nulo), quarentena (off-grid) e deduplicação por (`id_veiculo`, `timestamp_posicao`).
- **Gold (Analytics):** agregações (ex.: velocidade média por “zonas” de lat/long) e fatos (lead time / pontualidade por veículo/linha/dia).
- **DataOps:** testes unitários (PyTest), lint (flake8) e workflow de CI/CD (GitHub Actions), com deploy via Databricks CLI (Workspace import).

## Estrutura
```
data-mobility-bh/
├── .github/workflows/deploy_pipeline.yml
├── config/endpoints.yml
├── src/
│   ├── ingestion/ckan_ingest.py
│   ├── transformation/silver.py
│   ├── transformation/gold.py
│   ├── utils/spark.py
│   └── utils/logging.py
├── scripts/run_pipeline.py
├── tests/test_transformations.py
├── requirements.txt
└── README.md
```

## Como executar (local)
> Requer Java 8/11/17, Python 3.11+ e Spark via PySpark.

1) Crie ambiente e instale dependências:
```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

2) Execute testes:
```bash
pytest -q
```

3) Rode o pipeline (exemplo local):
```bash
python scripts/run_pipeline.py --env dev
```

### Paths (local vs Databricks)
- Local: usa `file:///...` para Parquet/Delta (default em `data/`).
- Databricks: você pode configurar `dbfs:/mnt/...` no `config/endpoints.yml`.

## Configuração de endpoints e storage
Edite `config/endpoints.yml`:
- `ckan.base_url`
- `ckan.resource_id_posicao`
- `storage.*.bronze/silver/gold` para `dev/stg/prod`

## Notas de Qualidade
- **Deduplicação:** chave natural `id_veiculo + timestamp_posicao`.
- **Velocidade outlier:** descartamos `velocidade > 100` (ajuste conforme regra).
- **Off-grid:** coordenadas fora de BH podem ir para “quarentena” (Bronze) — veja `silver.py`.

## CI/CD
Workflow em `.github/workflows/deploy_pipeline.yml`:
- lint → testes → deploy (somente `main`)
- deploy exemplificado com `databricks workspace import_dir`.

---
Autor: José Wanderson (2026)

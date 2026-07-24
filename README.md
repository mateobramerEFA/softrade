# Penta Trade

## Setup local

```bash
python -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows

pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

App en: http://localhost:8000
API docs en: http://localhost:8000/docs

## Estructura de la base de datos

```sql
exportaciones (ncm, pais, mes, vol, fob)  -- PK: (ncm, pais, mes)
cargas        (id, filename, sheet, registros, periodo_from, periodo_to, snapshot, timestamp)
```

## Flujo de carga

```
POST /api/preview   →  valida + parsea, devuelve token y resumen
POST /api/confirm/{token}  →  snapshot + UPSERT en SQLite
POST /api/discard/{token}  →  descarta sin tocar la DB
POST /api/rollback/{snapshot}  →  restaura a un punto anterior
```

## Deploy en Azure

```bash
az group create --name penta-trade-rg --location eastus

az appservice plan create \
  --name penta-trade-plan \
  --resource-group penta-trade-rg \
  --sku B1 --is-linux

az webapp create \
  --name penta-trade-app \
  --resource-group penta-trade-rg \
  --plan penta-trade-plan \
  --runtime "PYTHON:3.11"

az webapp config set \
  --name penta-trade-app \
  --resource-group penta-trade-rg \
  --startup-file "uvicorn app.main:app --host 0.0.0.0 --port 8000"

az webapp up --name penta-trade-app --resource-group penta-trade-rg
```

**Nota sobre persistencia en Azure:** el tier B1 tiene sistema de archivos persistente, pero para producción real conviene migrar `penta.db` a Azure SQL o guardar los snapshots en Blob Storage.

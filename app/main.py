from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
import sqlite3, io, json, shutil
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from collections import defaultdict

app = FastAPI(title="Penta Trade API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / "data"
SNAP_DIR    = DATA_DIR / "snapshots"
STAGING_DIR = DATA_DIR / "staging"
DB_PATH     = DATA_DIR / "penta.db"

for d in [DATA_DIR, SNAP_DIR, STAGING_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Database ───────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

@contextmanager
def db():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS exportaciones (
                ncm     TEXT NOT NULL,
                pais    TEXT NOT NULL,
                mes     TEXT NOT NULL,
                vol     REAL NOT NULL CHECK(vol > 0),
                fob     REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (ncm, pais, mes)
            );

            CREATE INDEX IF NOT EXISTS idx_exp_ncm_mes  ON exportaciones(ncm, mes);
            CREATE INDEX IF NOT EXISTS idx_exp_pais_mes ON exportaciones(pais, mes);
            CREATE INDEX IF NOT EXISTS idx_exp_mes      ON exportaciones(mes);

            CREATE TABLE IF NOT EXISTS cargas (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                filename     TEXT NOT NULL,
                sheet        TEXT NOT NULL,
                registros    INTEGER NOT NULL,
                omitidos     INTEGER NOT NULL DEFAULT 0,
                periodo_from TEXT,
                periodo_to   TEXT,
                snapshot     TEXT,
                timestamp    TEXT NOT NULL
            );
        """)

init_db()

# ── Validación ─────────────────────────────────────────────────────────────
REQUIRED_COLS = {
    "fecha": ["fecha"],
    "ncm":   ["ncm"],
    "pais":  ["destino", "país de destino", "pais de destino", "país", "pais"],
    "vol":   ["cantidad3", "cantidad"],
    "fob":   ["fob u", "u$s fob", "fob"],
}

def find_col(columns, *keywords):
    cols_lower = [c.strip().lower() for c in columns]
    for kw in keywords:
        matches = [columns[i] for i, c in enumerate(cols_lower) if kw in c]
        if matches:
            return matches[0]
    return None

def validate_columns(df):
    cols    = list(df.columns)
    mapping = {}
    errors  = []
    for field, aliases in REQUIRED_COLS.items():
        found = find_col(cols, *aliases)
        if found:
            mapping[field] = found
        elif field != "fob":
            errors.append({
                "field":   field,
                "aliases": aliases,
                "message": f"No se encontró '{field}'. Se buscó: {', '.join(aliases)}"
            })
    if errors:
        raise HTTPException(422, {
            "error":         "Columnas requeridas no encontradas",
            "detail":        errors,
            "columns_found": cols,
            "hint":          "Verificá que estás subiendo la hoja correcta (generalmente 'Strade')"
        })
    return mapping

def normalize_ncm(raw):
    return str(raw).replace(".", "").replace("-", "").replace(" ", "").upper().strip()

def parse_rows(df, mapping):
    rows, skipped = [], 0
    col_fecha = mapping["fecha"]
    col_ncm   = mapping["ncm"]
    col_pais  = mapping["pais"]
    col_vol   = mapping["vol"]
    col_fob   = mapping.get("fob")

    for _, r in df.iterrows():
        try:
            fecha = pd.to_datetime(r[col_fecha], dayfirst=True)
            if not (2010 <= fecha.year <= 2035):
                skipped += 1; continue
            ncm  = normalize_ncm(str(r[col_ncm]))
            pais = str(r[col_pais]).strip()
            vol  = float(r[col_vol])
            fob  = float(r[col_fob]) if col_fob and pd.notna(r.get(col_fob)) else 0.0
            if not ncm or not pais or vol <= 0:
                skipped += 1; continue
            mes = f"{fecha.year}-{fecha.month:02d}"
            rows.append((ncm, pais, mes, vol, fob))
        except Exception:
            skipped += 1
    return rows, skipped

def snapshot_db(label):
    if not DB_PATH.exists():
        return None
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"penta_{ts}_{label}.db"
    shutil.copy2(DB_PATH, SNAP_DIR / name)
    return name

# ── Endpoints ──────────────────────────────────────────────────────────────

@app.get("/api/status")
def status():
    with db() as conn:
        records  = conn.execute("SELECT COUNT(*) FROM exportaciones").fetchone()[0]
        products = conn.execute("SELECT COUNT(DISTINCT ncm) FROM exportaciones").fetchone()[0]
        markets  = conn.execute("SELECT COUNT(DISTINCT pais) FROM exportaciones").fetchone()[0]
        period   = conn.execute("SELECT MIN(mes), MAX(mes) FROM exportaciones").fetchone()
        uploads  = conn.execute(
            "SELECT * FROM cargas ORDER BY timestamp DESC LIMIT 10"
        ).fetchall()
        snaps    = sorted([f.name for f in SNAP_DIR.glob("*.db")], reverse=True)[:10]

    return {
        "records":     records,
        "products":    products,
        "markets":     markets,
        "period_from": period[0],
        "period_to":   period[1],
        "uploads":     [dict(u) for u in uploads],
        "snapshots":   snaps,
    }


@app.post("/api/preview")
async def preview_upload(file: UploadFile = File(...), sheet: str = None):
    """Valida y parsea el Excel SIN guardarlo. Devuelve token para confirmar."""
    contents = await file.read()
    try:
        wb = pd.ExcelFile(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(400, f"No se pudo leer el archivo: {e}")

    sheet_name = sheet
    if not sheet_name:
        candidates = [s for s in wb.sheet_names if "strade" in s.lower()]
        sheet_name = candidates[0] if candidates else wb.sheet_names[0]

    try:
        raw = pd.read_excel(wb, sheet_name=sheet_name)
    except Exception as e:
        raise HTTPException(400, f"Error leyendo hoja '{sheet_name}': {e}")

    mapping      = validate_columns(raw)
    rows, skipped = parse_rows(raw, mapping)

    if not rows:
        raise HTTPException(422, {
            "error": "No se pudo procesar ninguna fila válida.",
            "hint":  f"{skipped} filas descartadas. Revisá fechas y volúmenes."
        })

    # Guardar staging como JSON
    token   = datetime.now().strftime("%Y%m%d_%H%M%S")
    staging = [{"ncm":r[0],"pais":r[1],"mes":r[2],"vol":r[3],"fob":r[4]} for r in rows]
    (STAGING_DIR / f"{token}.json").write_text(
        json.dumps({"filename": file.filename, "sheet": sheet_name, "rows": staging})
    )

    # Resumen por NCM
    by_ncm = defaultdict(lambda: {"vol": 0, "registros": 0, "paises": set()})
    meses  = set()
    for ncm, pais, mes, vol, _ in rows:
        by_ncm[ncm]["vol"]      += vol
        by_ncm[ncm]["registros"] += 1
        by_ncm[ncm]["paises"].add(pais)
        meses.add(mes)

    return {
        "ok":          True,
        "token":       token,
        "sheet":       sheet_name,
        "sheets":      wb.sheet_names,
        "filename":    file.filename,
        "loaded":      len(rows),
        "skipped":     skipped,
        "period_from": min(meses),
        "period_to":   max(meses),
        "products":    len(by_ncm),
        "markets":     len({r[1] for r in rows}),
        "by_ncm": [
            {"ncm": k, "vol_total": round(v["vol"], 1),
             "registros": v["registros"], "paises": len(v["paises"])}
            for k, v in by_ncm.items()
        ],
        "columns_mapped": mapping,
    }


@app.post("/api/confirm/{token}")
def confirm_upload(token: str):
    """Snapshot + UPSERT en la base de datos."""
    staging_path = STAGING_DIR / f"{token}.json"
    if not staging_path.exists():
        raise HTTPException(404, "Token inválido o expirado. Volvé a subir el archivo.")

    data = json.loads(staging_path.read_text())
    rows = [(r["ncm"], r["pais"], r["mes"], r["vol"], r["fob"]) for r in data["rows"]]

    # Snapshot antes de tocar la DB
    snap = snapshot_db("pre_upload")

    with db() as conn:
        # UPSERT — si (ncm, pais, mes) ya existe, suma el volumen
        conn.executemany("""
            INSERT INTO exportaciones (ncm, pais, mes, vol, fob)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ncm, pais, mes) DO UPDATE SET
                vol = vol + excluded.vol,
                fob = fob + excluded.fob
        """, rows)

        meses = [r[2] for r in rows]
        conn.execute("""
            INSERT INTO cargas (filename, sheet, registros, omitidos, periodo_from, periodo_to, snapshot, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (data["filename"], data["sheet"], len(rows), 0,
              min(meses), max(meses), snap, datetime.now().isoformat()))

    staging_path.unlink()

    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM exportaciones").fetchone()[0]

    return {"ok": True, "inserted": len(rows), "total_db": total, "snapshot": snap}


@app.post("/api/discard/{token}")
def discard_upload(token: str):
    p = STAGING_DIR / f"{token}.json"
    if p.exists(): p.unlink()
    return {"ok": True, "message": "Carga descartada."}


@app.post("/api/rollback/{snapshot_name}")
def rollback(snapshot_name: str):
    snap_path = SNAP_DIR / snapshot_name
    if not snap_path.exists():
        raise HTTPException(404, f"Snapshot '{snapshot_name}' no encontrado.")
    snapshot_db("pre_rollback")
    shutil.copy2(snap_path, DB_PATH)
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM exportaciones").fetchone()[0]
    return {"ok": True, "restored": snapshot_name, "records": total}


@app.get("/api/products")
def get_products():
    with db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT ncm FROM exportaciones ORDER BY ncm"
        ).fetchall()
    return {"products": [r[0] for r in rows]}


@app.get("/api/markets/{ncm}")
def get_markets(ncm: str):
    with db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT pais FROM exportaciones WHERE ncm=? ORDER BY pais",
            (ncm.upper(),)
        ).fetchall()
    return {"markets": [r[0] for r in rows]}


@app.get("/api/data/{ncm}")
def get_data(ncm: str, pais: str = None):
    with db() as conn:
        if pais:
            rows = conn.execute(
                "SELECT pais, mes, vol, fob FROM exportaciones WHERE ncm=? AND pais=? ORDER BY mes",
                (ncm.upper(), pais)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT pais, mes, vol, fob FROM exportaciones WHERE ncm=? ORDER BY pais, mes",
                (ncm.upper(),)
            ).fetchall()

    result = {}
    for r in rows:
        p, m = r["pais"], r["mes"]
        if p not in result: result[p] = {}
        result[p][m] = {"vol": round(r["vol"], 2), "fob": round(r["fob"], 2)}
    return {"data": result}


@app.get("/api/summary")
def get_summary():
    """Todos los datos para el frontend — ncm > pais > mes > vol."""
    with db() as conn:
        rows = conn.execute(
            "SELECT ncm, pais, mes, vol FROM exportaciones ORDER BY ncm, pais, mes"
        ).fetchall()

    result = {}
    for r in rows:
        ncm, pais, mes = r["ncm"], r["pais"], r["mes"]
        if ncm not in result:            result[ncm] = {}
        if pais not in result[ncm]:      result[ncm][pais] = {}
        result[ncm][pais][mes] = round(r["vol"], 2)
    return {"data": result}


# Frontend estático
frontend = BASE_DIR / "frontend"
if frontend.exists():
    app.mount("/", StaticFiles(directory=str(frontend), html=True), name="frontend")

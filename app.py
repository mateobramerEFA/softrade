from flask import Flask, request, jsonify, render_template
import io, json, shutil, os
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from collections import defaultdict
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
CONN_STR    = os.environ.get('AZURE_SQL_CONN_STR', '')
USE_SQLITE  = not CONN_STR  # fallback local si no hay Azure SQL

BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
SNAP_DIR    = DATA_DIR / "snapshots"
STAGING_DIR = BASE_DIR / "staging"
DB_PATH     = DATA_DIR / "penta.db"

for d in [DATA_DIR, SNAP_DIR, STAGING_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Database ───────────────────────────────────────────────────────────────
@contextmanager
def db():
    if USE_SQLITE:
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    else:
        import pyodbc
        conn = pyodbc.connect(CONN_STR)
        conn.autocommit = False
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

def placeholder(n=1):
    """? para SQLite, ? también para pyodbc — ambos usan ?"""
    return ','.join(['?' for _ in range(n)])

def rows_to_dict(rows):
    """Convierte rows de sqlite3.Row o pyodbc.Row a lista de dicts."""
    if not rows:
        return []
    if hasattr(rows[0], 'keys'):
        return [dict(r) for r in rows]
    # pyodbc — usar cursor.description
    return rows

def init_db():
    with db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sysobjects
                WHERE name='softrade_exportaciones' AND xtype='U'
            )
            CREATE TABLE softrade_exportaciones (
                identificador NVARCHAR(100) NOT NULL,
                item          NVARCHAR(50)  NOT NULL,
                ncm           NVARCHAR(50)  NOT NULL,
                pais          NVARCHAR(100) NOT NULL,
                mes           NVARCHAR(7)   NOT NULL,
                vol           FLOAT         NOT NULL,
                fob           FLOAT         NOT NULL DEFAULT 0,
                PRIMARY KEY (identificador, item)
            )
        """ if not USE_SQLITE else """
            CREATE TABLE IF NOT EXISTS softrade_exportaciones (
                identificador TEXT NOT NULL,
                item          TEXT NOT NULL,
                ncm           TEXT NOT NULL,
                pais          TEXT NOT NULL,
                mes           TEXT NOT NULL,
                vol           REAL NOT NULL CHECK(vol > 0),
                fob           REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (identificador, item)
            )
        """)

        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sysobjects
                WHERE name='softrade_cargas' AND xtype='U'
            )
            CREATE TABLE softrade_cargas (
                id           INT IDENTITY(1,1) PRIMARY KEY,
                filename     NVARCHAR(255) NOT NULL,
                sheet        NVARCHAR(100) NOT NULL,
                registros    INT           NOT NULL,
                omitidos     INT           NOT NULL DEFAULT 0,
                duplicados   INT           NOT NULL DEFAULT 0,
                periodo_from NVARCHAR(7),
                periodo_to   NVARCHAR(7),
                snapshot     NVARCHAR(255),
                timestamp    NVARCHAR(50)  NOT NULL
            )
        """ if not USE_SQLITE else """
            CREATE TABLE IF NOT EXISTS softrade_cargas (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                filename     TEXT NOT NULL,
                sheet        TEXT NOT NULL,
                registros    INT  NOT NULL,
                omitidos     INT  NOT NULL DEFAULT 0,
                duplicados   INT  NOT NULL DEFAULT 0,
                periodo_from TEXT,
                periodo_to   TEXT,
                snapshot     TEXT,
                timestamp    TEXT NOT NULL
            )
        """)

        # Índices — solo SQLite los necesita explícitos
        if USE_SQLITE:
            cursor.executescript("""
                CREATE INDEX IF NOT EXISTS idx_exp_ncm_mes  ON softrade_exportaciones(ncm, mes);
                CREATE INDEX IF NOT EXISTS idx_exp_pais_mes ON softrade_exportaciones(pais, mes);
                CREATE INDEX IF NOT EXISTS idx_exp_ncm_pais ON softrade_exportaciones(ncm, pais);
            """)

init_db()

# ── Validación ─────────────────────────────────────────────────────────────
REQUIRED_COLS = {
    "identificador": ["identificador"],
    "item":          ["item"],
    "fecha":         ["fecha"],
    "ncm":           ["ncm"],
    "pais":          ["país de destino", "pais de destino", "destino", "país", "pais"],
    "vol":           ["cantidad3", "cantidad"],
    "fob":           ["fob u", "u$s fob", "fob"],
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
        elif field not in ("fob", "item"):
            errors.append({
                "field":   field,
                "aliases": aliases,
                "message": f"No se encontró '{field}'. Se buscó: {', '.join(aliases)}"
            })
    return mapping, errors

def normalize_ncm(raw):
    return str(raw).replace(".", "").replace("-", "").replace(" ", "").upper().strip()

def parse_rows(df, mapping):
    rows, skipped = [], 0
    col_id    = mapping.get("identificador")
    col_item  = mapping.get("item")
    col_fecha = mapping["fecha"]
    col_ncm   = mapping["ncm"]
    col_pais  = mapping["pais"]
    col_vol   = mapping["vol"]
    col_fob   = mapping.get("fob")

    for idx, r in df.iterrows():
        try:
            fecha = pd.to_datetime(r[col_fecha], dayfirst=True)
            if not (2010 <= fecha.year <= 2035):
                skipped += 1; continue
            identificador = str(r[col_id]).strip() if col_id else str(idx)
            item          = str(r[col_item]).strip() if col_item else "1"
            ncm           = normalize_ncm(str(r[col_ncm]))
            pais          = str(r[col_pais]).strip()
            vol           = float(r[col_vol])
            fob           = float(r[col_fob]) if col_fob and pd.notna(r.get(col_fob)) else 0.0
            mes           = f"{fecha.year}-{fecha.month:02d}"
            if not ncm or not pais or vol <= 0:
                skipped += 1; continue
            rows.append((identificador, item, ncm, pais, mes, vol, fob))
        except Exception:
            skipped += 1
    return rows, skipped

def snapshot_db(label):
    if not USE_SQLITE or not DB_PATH.exists():
        return None
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"penta_{ts}_{label}.db"
    shutil.copy2(DB_PATH, SNAP_DIR / name)
    return name

# ── Frontend ───────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

# ── API ────────────────────────────────────────────────────────────────────
@app.route("/api/status")
def status():
    with db() as conn:
        cur = conn.cursor()
        records  = cur.execute("SELECT COUNT(*) FROM softrade_exportaciones").fetchone()[0]
        products = cur.execute("SELECT COUNT(DISTINCT ncm) FROM softrade_exportaciones").fetchone()[0]
        markets  = cur.execute("SELECT COUNT(DISTINCT pais) FROM softrade_exportaciones").fetchone()[0]
        period   = cur.execute("SELECT MIN(mes), MAX(mes) FROM softrade_exportaciones").fetchone()
        cur.execute("SELECT TOP 10 * FROM softrade_cargas ORDER BY timestamp DESC" if not USE_SQLITE
                    else "SELECT * FROM softrade_cargas ORDER BY timestamp DESC LIMIT 10")
        uploads  = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
        snaps    = sorted([f.name for f in SNAP_DIR.glob("*.db")], reverse=True)[:10] if USE_SQLITE else []

    return jsonify({
        "records":     records,
        "products":    products,
        "markets":     markets,
        "period_from": period[0],
        "period_to":   period[1],
        "uploads":     uploads,
        "snapshots":   snaps,
        "backend":     "sqlite" if USE_SQLITE else "azure_sql",
    })


@app.route("/api/preview", methods=["POST"])
def preview_upload():
    if "file" not in request.files:
        return jsonify({"error": "No se envió ningún archivo"}), 400

    file       = request.files["file"]
    sheet_name = request.form.get("sheet")

    try:
        wb = pd.ExcelFile(io.BytesIO(file.read()))
    except Exception as e:
        return jsonify({"error": f"No se pudo leer el archivo: {e}"}), 400

    if not sheet_name:
        candidates = [s for s in wb.sheet_names if "strade" in s.lower()]
        sheet_name = candidates[0] if candidates else wb.sheet_names[0]

    try:
        raw = pd.read_excel(wb, sheet_name=sheet_name)
    except Exception as e:
        return jsonify({"error": f"Error leyendo hoja '{sheet_name}': {e}"}), 400

    mapping, errors = validate_columns(raw)
    if errors:
        return jsonify({
            "error":         "Columnas requeridas no encontradas",
            "detail":        errors,
            "columns_found": list(raw.columns),
            "hint":          "Verificá que estás subiendo la hoja correcta (generalmente 'Strade')"
        }), 422

    rows, skipped = parse_rows(raw, mapping)
    if not rows:
        return jsonify({"error": "No se pudo procesar ninguna fila válida.", "hint": f"{skipped} descartadas."}), 422

    # Chequear duplicados contra la DB
    with db() as conn:
        cur = conn.cursor()
        existing = set()
        for r in rows:
            cur.execute("SELECT 1 FROM softrade_exportaciones WHERE identificador=? AND item=?", (r[0], r[1]))
            if cur.fetchone():
                existing.add((r[0], r[1]))

    new_rows  = [r for r in rows if (r[0], r[1]) not in existing]
    dup_count = len(existing)

    token   = datetime.now().strftime("%Y%m%d_%H%M%S")
    staging = [{"id": r[0], "item": r[1], "ncm": r[2], "pais": r[3],
                "mes": r[4], "vol": r[5], "fob": r[6]} for r in new_rows]
    (STAGING_DIR / f"{token}.json").write_text(
        json.dumps({"filename": file.filename, "sheet": sheet_name,
                    "rows": staging, "duplicates": dup_count})
    )

    by_ncm = defaultdict(lambda: {"vol": 0, "registros": 0, "paises": set()})
    meses  = set()
    for r in rows:
        by_ncm[r[2]]["vol"]       += r[5]
        by_ncm[r[2]]["registros"] += 1
        by_ncm[r[2]]["paises"].add(r[3])
        meses.add(r[4])

    return jsonify({
        "ok":           True,
        "token":        token,
        "sheet":        sheet_name,
        "sheets":       wb.sheet_names,
        "filename":     file.filename,
        "loaded":       len(rows),
        "skipped":      skipped,
        "new":          len(new_rows),
        "duplicates":   dup_count,
        "period_from":  min(meses),
        "period_to":    max(meses),
        "products":     len(by_ncm),
        "markets":      len({r[3] for r in rows}),
        "by_ncm":       [{"ncm": k, "vol_total": round(v["vol"], 1),
                          "registros": v["registros"], "paises": len(v["paises"])}
                         for k, v in by_ncm.items()],
        "columns_mapped": mapping,
    })


@app.route("/api/confirm/<token>", methods=["POST"])
def confirm_upload(token):
    staging_path = STAGING_DIR / f"{token}.json"
    if not staging_path.exists():
        return jsonify({"error": "Token inválido o expirado."}), 404

    data = json.loads(staging_path.read_text())
    rows = [(r["id"], r["item"], r["ncm"], r["pais"], r["mes"], r["vol"], r["fob"])
            for r in data["rows"]]

    snap = snapshot_db("pre_upload")

    inserted = 0
    with db() as conn:
        cur = conn.cursor()
        for r in rows:
            try:
                cur.execute("""
                    INSERT INTO softrade_exportaciones
                        (identificador, item, ncm, pais, mes, vol, fob)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, r)
                inserted += 1
            except Exception:
                pass  # duplicado — ignorar

        meses = [r[4] for r in rows] if rows else [""]
        cur.execute("""
            INSERT INTO softrade_cargas
                (filename, sheet, registros, omitidos, duplicados,
                 periodo_from, periodo_to, snapshot, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (data["filename"], data["sheet"], inserted, 0,
              data.get("duplicates", 0), min(meses), max(meses),
              snap, datetime.now().isoformat()))

    staging_path.unlink()

    with db() as conn:
        total = conn.cursor().execute("SELECT COUNT(*) FROM softrade_exportaciones").fetchone()[0]

    return jsonify({"ok": True, "inserted": inserted,
                    "duplicates": data.get("duplicates", 0), "total_db": total, "snapshot": snap})


@app.route("/api/discard/<token>", methods=["POST"])
def discard_upload(token):
    p = STAGING_DIR / f"{token}.json"
    if p.exists(): p.unlink()
    return jsonify({"ok": True})


@app.route("/api/rollback/<snapshot_name>", methods=["POST"])
def rollback(snapshot_name):
    if not USE_SQLITE:
        return jsonify({"error": "Rollback solo disponible en modo local."}), 400
    snap_path = SNAP_DIR / snapshot_name
    if not snap_path.exists():
        return jsonify({"error": f"Snapshot '{snapshot_name}' no encontrado."}), 404
    snapshot_db("pre_rollback")
    shutil.copy2(snap_path, DB_PATH)
    with db() as conn:
        total = conn.cursor().execute("SELECT COUNT(*) FROM softrade_exportaciones").fetchone()[0]
    return jsonify({"ok": True, "restored": snapshot_name, "records": total})


@app.route("/api/products")
def get_products():
    with db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT ncm FROM softrade_exportaciones ORDER BY ncm")
        rows = cur.fetchall()
    return jsonify({"products": [r[0] for r in rows]})


@app.route("/api/markets/<ncm>")
def get_markets(ncm):
    with db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT pais FROM softrade_exportaciones WHERE ncm=? ORDER BY pais",
                    (ncm.upper(),))
        rows = cur.fetchall()
    return jsonify({"markets": [r[0] for r in rows]})


@app.route("/api/data/<ncm>")
def get_data(ncm):
    pais = request.args.get("pais")
    with db() as conn:
        cur = conn.cursor()
        if pais:
            cur.execute("""
                SELECT pais, mes, SUM(vol) as vol, SUM(fob) as fob
                FROM softrade_exportaciones
                WHERE ncm=? AND pais=?
                GROUP BY pais, mes ORDER BY mes
            """, (ncm.upper(), pais))
        else:
            cur.execute("""
                SELECT pais, mes, SUM(vol) as vol, SUM(fob) as fob
                FROM softrade_exportaciones
                WHERE ncm=?
                GROUP BY pais, mes ORDER BY pais, mes
            """, (ncm.upper(),))
        rows = cur.fetchall()

    result = {}
    for r in rows:
        p, m = r[0], r[1]
        if p not in result: result[p] = {}
        result[p][m] = {"vol": round(r[2], 2), "fob": round(r[3], 2)}
    return jsonify({"data": result})


@app.route("/api/summary")
def get_summary():
    with db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ncm, pais, mes, SUM(vol) as vol
            FROM softrade_exportaciones
            GROUP BY ncm, pais, mes
            ORDER BY ncm, pais, mes
        """)
        rows = cur.fetchall()

    result = {}
    for r in rows:
        ncm, pais, mes, vol = r[0], r[1], r[2], r[3]
        if ncm not in result:       result[ncm] = {}
        if pais not in result[ncm]: result[ncm][pais] = {}
        result[ncm][pais][mes] = round(vol, 2)
    return jsonify({"data": result})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
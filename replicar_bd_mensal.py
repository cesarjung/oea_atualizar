# replicar_bd_mensal.py
# 1) Lê Historico_Mensal.csv do Drive e cola em BD_Mensal!A1 (A:AK) exatamente como está.
# 2) Converte SOMENTE:
#    - A, D, AK -> data (serial do Google Sheets)
#    - E, L..Y  -> número
# 3) Grava timestamp em RESUMO!A2 (formato dd/mm/yyyy HH:mm, America/Sao_Paulo).
# Compatível com gspread 6.x (update(values, range_name=...)).

import io
import re
import sys
import time
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

try:
    from gspread_formatting import format_cell_range, CellFormat, NumberFormat
    HAS_FMT = True
except Exception:
    HAS_FMT = False

# Timezone
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ = None

# ===================== CONFIG =====================
CAMINHO_CRED = "credenciais.json"

FOLDER_ID = "1108v_R_-KpYXclfUPaXsRqzsyQ0tiMjh"  # pasta do Drive
CSV_NAME  = "Historico_Mensal.csv"

DEST_SPREADSHEET_ID = "1-ZguV_LFofJ2F-Emn0UQQx1UfVOcKpTXZb1VryVeds4"
DEST_WORKSHEET = "BD_Mensal"

RANGE_CLEAR = "A:AK"    # limpa apenas conteúdo A..AK
MAX_COLS = 37           # limite máximo (AK)
CHUNK_ROWS = 2000
VALUE_INPUT_OPTION_RAW = "RAW"

MAX_API_RETRIES = 6
BASE_SLEEP = 2.0

# Colunas a tratar (1-based)
COLS_DATE = {1, 4, 37}                 # A, D, AK
COLS_NUM  = {5} | set(range(12, 26))   # E, L..Y

# ===================== AUTH =====================
def auth_clients():
    scopes = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_file(CAMINHO_CRED, scopes=scopes)
    gc = gspread.authorize(creds)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    return gc, drive

# ===================== DRIVE =====================
def get_latest_csv_from_folder(drive, folder_id: str, name: str) -> Optional[Tuple[str, str]]:
    query = (
        f"'{folder_id}' in parents and name = '{name}' and "
        f"mimeType = 'text/csv' and trashed = false"
    )
    resp = drive.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=5,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
    ).execute()
    files = resp.get("files", [])
    if not files:
        return None
    f = files[0]
    return f["id"], f["modifiedTime"]

def download_file_content(drive, file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()

# ===================== SHEETS HELPERS =====================
def safe_call(fn, desc="chamada API"):
    for i in range(1, MAX_API_RETRIES + 1):
        try:
            return fn()
        except APIError as e:
            wait = BASE_SLEEP * i
            print(f"⚠️  Falha na {desc}: {e}. Tentativa {i}/{MAX_API_RETRIES}. Aguardando {wait:.1f}s...")
            time.sleep(wait)
        except Exception as e:
            wait = BASE_SLEEP * i
            print(f"⚠️  Erro inesperado na {desc}: {e}. Tentativa {i}/{MAX_API_RETRIES}. Aguardando {wait:.1f}s...")
            time.sleep(wait)
    raise RuntimeError(f"Falhou: {desc}")

def ensure_min_rows(ws, required_rows: int):
    try:
        current_rows = ws.row_count
    except Exception:
        current_rows = None
    if current_rows is None or required_rows > current_rows:
        delta = required_rows - (current_rows or 0)
        safe_call(lambda: ws.add_rows(delta) if current_rows else ws.resize(rows=required_rows),
                  "aumentar linhas")

def batch_clear(ws, a1_range: str):
    safe_call(lambda: ws.batch_clear([a1_range]), f"limpeza {a1_range}")

def update_chunk(ws, start_row: int, start_col: int, values, value_input_option="RAW"):
    import gspread.utils as gu
    if not values:
        return
    end_row = start_row + len(values) - 1
    end_col = start_col + (len(values[0]) - 1 if values and values[0] else 0)
    rng = f"{gu.rowcol_to_a1(start_row, start_col)}:{gu.rowcol_to_a1(end_row, end_col)}"
    # gspread 6.x: valores primeiro, range_name nomeado
    safe_call(lambda: ws.update(values, range_name=rng, value_input_option=value_input_option),
              f"update {rng}")

# ===================== CONVERSÕES =====================
DATE_PATTERNS = [
    "%d/%m/%Y",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
]

def parse_to_datetime(val: str):
    s = str(val).strip()
    if not s or s.lower() in ("nan","none","null","-"):
        return None
    s2 = s.replace("T", " ").replace("  ", " ")
    for fmt in DATE_PATTERNS:
        try:
            return datetime.strptime(s2, fmt)
        except:
            pass
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?\s*$", s2)
    if m:
        dd, mm, yyyy = map(int, m.group(1,2,3))
        hh = int(m.group(4) or 0); mi = int(m.group(5) or 0); ss = int(m.group(6) or 0)
        try:
            return datetime(yyyy, mm, dd, hh, mi, ss)
        except:
            return None
    return None

def datetime_to_sheets_serial(dt: datetime) -> float:
    base = datetime(1899, 12, 30)
    delta = dt - base
    return delta.days + (delta.seconds + delta.microseconds/1e6)/86400.0

def to_float_br_us(val: str):
    s = str(val).strip()
    if s == "" or s.lower() in ("nan","none","null","-"):
        return None
    s2 = re.sub(r"[^\d,\.\-]", "", s)
    if s2 == "":
        return None
    s2 = re.sub(r"\.(?=\d{3}(?:\D|$))", "", s2)
    s2 = s2.replace(",", ".")
    try:
        return float(s2)
    except:
        return None

# ===================== TIMESTAMP RESUMO (A2, dd/mm/yyyy HH:mm) =====================
def gravar_timestamp_resumo(sh):
    """Grava timestamp em RESUMO!A2 no formato dd/mm/yyyy HH:mm (America/Sao_Paulo), sem segundos."""
    ts = (datetime.now(TZ) if TZ else datetime.now()).strftime("%d/%m/%Y %H:%M")
    try:
        try:
            ws_resumo = sh.worksheet("RESUMO")
        except WorksheetNotFound:
            ws_resumo = sh.add_worksheet(title="RESUMO", rows=10, cols=5)
        # gspread 6.x — sempre 2D + range_name
        safe_call(lambda: ws_resumo.update([[ts]], range_name="A2", value_input_option="RAW"),
                  "atualizar RESUMO!A2")
        # (opcional) formatar A2 como data+hora sem segundos
        if HAS_FMT:
            try:
                fmt = CellFormat(numberFormat=NumberFormat(type="DATE_TIME", pattern="dd/mm/yyyy HH:mm"))
                format_cell_range(ws_resumo, "A2", fmt)
            except Exception:
                pass
        print(f"🕒 RESUMO!A2 atualizado com '{ts}'.")
    except Exception as e:
        print(f"⚠️  Não foi possível atualizar RESUMO!A2: {e}")

# ===================== MAIN =====================
def main():
    print("🔐 Autenticando...")
    gc, drive = auth_clients()
    print("✅ Autenticado.\n")

    print("🔎 Buscando 'Historico_Mensal.csv' na pasta do Drive…")
    res = get_latest_csv_from_folder(drive, FOLDER_ID, CSV_NAME)
    if not res:
        print("❌ Não encontrei 'Historico_Mensal.csv' na pasta informada.")
        sys.exit(1)
    file_id, mtime = res
    print(f"📝 Arquivo encontrado. Última modificação: {mtime}")

    print("📥 Baixando CSV…")
    content = download_file_content(drive, file_id)
    print(f"✅ {len(content)} bytes baixados.\n")

    # ===== Leitura do CSV =====
    df = None
    try:
        df = pd.read_csv(
            io.BytesIO(content),
            sep=None, engine="python",
            dtype=str, encoding="utf-8-sig",
            keep_default_na=False, na_filter=False,
        )
    except Exception:
        df = None

    if df is None or df.shape[1] == 1:
        for sep in [";", ","]:
            try:
                tmp = pd.read_csv(
                    io.BytesIO(content),
                    sep=sep, dtype=str, encoding="utf-8-sig",
                    keep_default_na=False, na_filter=False,
                )
                if tmp.shape[1] == 1 and sep == ";":
                    continue
                df = tmp
                break
            except Exception:
                df = None

    if df is None:
        print("❌ Falha ao ler o CSV.")
        sys.exit(1)

    if df.shape[1] > MAX_COLS:
        df = df.iloc[:, :MAX_COLS]

    headers = list(df.columns)
    num_cols = min(df.shape[1], MAX_COLS)

    print(f"🧭 Colunas detectadas: {df.shape[1]}")
    for idx, name in enumerate(df.columns, start=1):
        if 31 <= idx <= 37:
            print(f"   {idx:02d} → {name}")

    header_row = headers[:num_cols]
    data_rows = df.iloc[:, :num_cols].values.tolist()
    data = [header_row] + data_rows

    print(f"\n📂 Abrindo destino: {DEST_SPREADSHEET_ID} › {DEST_WORKSHEET}")
    try:
        sh = gc.open_by_key(DEST_SPREADSHEET_ID)
        try:
            ws = sh.worksheet(DEST_WORKSHEET)
        except WorksheetNotFound:
            print("🆕 Aba não existe. Criando…")
            ws = sh.add_worksheet(title=DEST_WORKSHEET, rows=10, cols=MAX_COLS)
    except Exception as e:
        print(f"❌ Erro ao abrir destino: {e}")
        sys.exit(1)

    total_rows = len(data)
    print(f"📏 Linhas (inclui cabeçalho): {total_rows} | Colunas: {num_cols}")

    print("🧹 Limpando A:AK (somente conteúdo)…")
    batch_clear(ws, RANGE_CLEAR)

    ensure_min_rows(ws, max(total_rows, 50))

    print("🚀 Colando conteúdo (1:1 do CSV)…")
    start = 1
    for i in range(0, total_rows, CHUNK_ROWS):
        chunk = data[i : i + CHUNK_ROWS]
        print(f"   • Linhas {i+1}–{i+len(chunk)}")
        update_chunk(ws, start_row=start + i, start_col=1,
                     values=chunk, value_input_option=VALUE_INPUT_OPTION_RAW)

    # ===== Conversões seletivas =====
    n_rows = len(data_rows)  # sem cabeçalho
    if n_rows == 0:
        print("ℹ️ Sem linhas de dados; nada para converter.")
        gravar_timestamp_resumo(sh)
        print("\n✅ Concluído.")
        return

    def update_col_from_list(col_idx_1based: int, values_list):
        col_matrix = [[x] for x in values_list]
        update_chunk(ws, start_row=2, start_col=col_idx_1based,
                     values=col_matrix, value_input_option=VALUE_INPUT_OPTION_RAW)

    for c in sorted(COLS_DATE):
        if c > num_cols:
            continue
        col_vals = [row[c-1] for row in data_rows]
        converted = []
        for v in col_vals:
            dt = parse_to_datetime(v)
            converted.append(datetime_to_sheets_serial(dt) if dt else v)
        update_col_from_list(c, converted)
        print(f"📅 Coluna {c} (data) convertida onde possível.")

    for c in sorted(COLS_NUM):
        if c > num_cols:
            continue
        col_vals = [row[c-1] for row in data_rows]
        conv = []
        for v in col_vals:
            f = to_float_br_us(v)
            conv.append(f if f is not None else v)
        update_col_from_list(c, conv)
        print(f"🔢 Coluna {c} (número) convertida onde possível.")

    if HAS_FMT:
        try:
            fmt_date = CellFormat(numberFormat=NumberFormat(type="DATE", pattern="dd/mm/yyyy"))
            col_letters = {1: "A", 4: "D", 37: "AK"}
            for idx, letter in col_letters.items():
                if idx <= num_cols:
                    format_cell_range(ws, f"{letter}:{letter}", fmt_date)
        except Exception as e:
            print(f"⚠️  Não consegui aplicar formatação de data: {e}")

    gravar_timestamp_resumo(sh)
    print("\n✅ Concluído! A:AK limpo e colado; **AG preservada**; só A, D, AK (data) e E, L..Y (número) convertidas.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário.")

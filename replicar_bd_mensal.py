# replicar_esteira_oea.py
# Lê cabeçalho (A3:AN3) e dados (A4:AN) da planilha origem e escreve em Base_Esteira:
# - A2 recebe o cabeçalho
# - A3 em diante recebe os dados (em blocos)
# - A1 recebe status textual
# Compatível com gspread 6.x (update(values, range_name=...)).

import time
from datetime import datetime

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials

# ===================== CONFIG =====================
CAMINHO_CRED         = "credenciais.json"
ORIGEM_SPREADSHEET   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ORIGEM_WORKSHEET     = "BD_Carteira"

DEST_SPREADSHEET     = "1-ZguV_LFofJ2F-Emn0UQQx1UfVOcKpTXZb1VryVeds4"
DEST_WORKSHEET       = "Base_Esteira"

COL_INICIO = 1     # A
COL_FIM    = 40    # AN (A..AN)
CHUNK_ROWS = 8000
MAX_API_RETRIES = 6
BASE_SLEEP = 2.0

# ===================== HELPERS =====================
def a1_range(col_start, row_start, col_end, row_end):
    import gspread.utils as gu
    return f"{gu.rowcol_to_a1(row_start, col_start)}:{gu.rowcol_to_a1(row_end, col_end)}"

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

def auth():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CAMINHO_CRED, scopes=scopes)
    return gspread.authorize(creds)

def write_status(ws, text):
    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    msg = f"{text} — {ts}"
    # gspread 6.x — sempre 2D + range_name
    safe_call(lambda: ws.update([[msg]], range_name="A1", value_input_option="RAW"),
              "escrever status em A1")

# ===================== MAIN =====================
def main():
    print("🔐 Autenticando...")
    gc = auth()
    print(f"✅ Autenticado. gspread={gspread.__version__}")

    # Abrir origem
    sh_src = gc.open_by_key(ORIGEM_SPREADSHEET)
    ws_src = sh_src.worksheet(ORIGEM_WORKSHEET)
    print(f"📂 Origem: {ORIGEM_SPREADSHEET} › {ORIGEM_WORKSHEET}")

    # Abrir destino
    sh_dst = gc.open_by_key(DEST_SPREADSHEET)
    try:
        ws_dst = sh_dst.worksheet(DEST_WORKSHEET)
    except WorksheetNotFound:
        ws_dst = sh_dst.add_worksheet(title=DEST_WORKSHEET, rows=100, cols=COL_FIM)
    print(f"📂 Destino: {DEST_SPREADSHEET} › {DEST_WORKSHEET}")

    # Status inicial
    try:
        write_status(ws_dst, "Atualizando Base_Esteira…")
    except Exception as e:
        print(f"⚠️ Falha ao escrever status em A1: {e}")

    # Ler cabeçalho e dados como valores nativos (sem apóstrofo)
    print("📥 Lendo cabeçalho (A3:AN3) como valores nativos…")
    header = safe_call(lambda: ws_src.get(a1_range(COL_INICIO, 3, COL_FIM, 3), value_render_option="UNFORMATTED_VALUE"),
                       "ler cabeçalho")
    header = header[0] if header else []

    print("📥 Lendo dados (A4:AN) como valores nativos…")
    data = safe_call(lambda: ws_src.get(a1_range(COL_INICIO, 4, COL_FIM, ws_src.row_count),
                                        value_render_option="UNFORMATTED_VALUE"),
                     "ler dados")
    # Remover linhas vazias finais
    while data and all(v in ("", None) for v in data[-1]):
        data.pop()

    n_rows = len(data)
    n_cols = COL_FIM - COL_INICIO + 1
    print(f"🔎 Linhas lidas: {n_rows} (sem contar cabeçalho) | Colunas: {n_cols} | ⏱️ leitura: ok")

    # Limpar destino (A:AN)
    print("🧹 Limpando destino (A:AN)…")
    safe_call(lambda: ws_dst.batch_clear(["A:AN"]), "limpeza A:AN")
    print("✅ Limpeza concluída.")

    # Garantir tamanho mínimo
    min_rows = max(3 + n_rows, 50)
    if ws_dst.row_count < min_rows:
        safe_call(lambda: ws_dst.add_rows(min_rows - ws_dst.row_count), "aumentar linhas destino")

    # Escrever cabeçalho em A2
    print("✍️ Gravando cabeçalho em A2…")
    rng_header = a1_range(COL_INICIO, 2, COL_FIM, 2)
    safe_call(lambda: ws_dst.update([header], range_name=rng_header, raw=True), "gravar cabeçalho")

    # Escrever dados em blocos a partir de A3
    if n_rows > 0:
        print(f"🚚 Gravando {n_rows} linhas em blocos de {CHUNK_ROWS}…")
        row_cursor = 3
        start_idx = 0
        while start_idx < n_rows:
            end_idx = min(start_idx + CHUNK_ROWS, n_rows)
            chunk = data[start_idx:end_idx]
            end_row = row_cursor + len(chunk) - 1
            rng_body = a1_range(COL_INICIO, row_cursor, COL_FIM, end_row)
            t0 = time.time()
            # gspread 6.x — valores primeiro, range_name nomeado
            safe_call(lambda: ws_dst.update(chunk, range_name=rng_body, raw=True), f"update {rng_body}")
            dt = time.time() - t0
            print(f"   • Gravado {row_cursor}-{end_row} ({len(chunk)} linhas) | ⏱️ {dt:.2f}s")
            row_cursor = end_row + 1
            start_idx = end_idx

    # Status final
    try:
        write_status(ws_dst, f"Base_Esteira atualizada com {n_rows} linhas")
    except Exception as e:
        print(f"⚠️ Falha ao escrever status em A1: {e}")

    print("🟢 Concluído.")

if __name__ == "__main__":
    main()

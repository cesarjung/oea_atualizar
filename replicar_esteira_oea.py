# -*- coding: utf-8 -*-
"""
Replica A:AN da aba BD_Carteira (linha 3 em diante, incluindo o cabeçalho da linha 3)
para a aba Base_Esteira em OUTRA planilha, colando em A2.
- Sem conversão manual (sem "tratar apóstrofos"): lê valores já nativos (número/serial)
- Limpa A:AN do destino
- Logs de cada etapa (leitura, limpeza, escrita, ETA)
"""

import time
from datetime import datetime
from typing import List

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

# ====== CONFIG ======
CAMINHO_CRED = "credenciais.json"

ID_ORIGEM   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM  = "BD_Carteira"

ID_DESTINO  = "1-ZguV_LFofJ2F-Emn0UQQx1UfVOcKpTXZb1VryVeds4"
ABA_DESTINO = "Base_Esteira"

COL_INICIO  = "A"
COL_FIM     = "AN"

CHUNK_ROWS  = 8000   # ajuste se quiser
# =====================

def auth():
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CAMINHO_CRED, scopes)
    return gspread.authorize(creds)

def a1_range(c1, r1, c2, r2):
    return f"{c1}{r1}:{c2}{r2}"

def normalize_width(rows: List[List], total_cols: int) -> List[List]:
    out = []
    for r in rows:
        r = list(r)
        if len(r) < total_cols:
            r += [""] * (total_cols - len(r))
        elif len(r) > total_cols:
            r = r[:total_cols]
        out.append(r)
    return out

def set_status(ws, text):
    try:
        ws.update([[text]], "A1", raw=True)
    except Exception as e:
        print(f"⚠️ Falha ao escrever status em A1: {e}")

def main():
    t0 = time.time()
    print("🔐 Autenticando...")
    gc = auth()
    print(f"✅ Autenticado. gspread={gspread.__version__}\n")

    sh_src = gc.open_by_key(ID_ORIGEM)
    sh_dst = gc.open_by_key(ID_DESTINO)
    ws_src = sh_src.worksheet(ABA_ORIGEM)
    ws_dst = sh_dst.worksheet(ABA_DESTINO)

    print(f"📂 Origem: {ID_ORIGEM} › {ABA_ORIGEM}")
    print(f"📂 Destino: {ID_DESTINO} › {ABA_DESTINO}")

    # Sinal imediato de vida no destino
    set_status(ws_dst, "⏱️ Em execução...")

    # -------- LEITURA --------
    t_read0 = time.time()
    print("📥 Lendo cabeçalho (A3:AN3) como valores nativos…")
    try:
        header_rows = ws_src.get(
            f"{COL_INICIO}3:{COL_FIM}3",
            value_render_option="UNFORMATTED_VALUE",
            date_time_render_option="SERIAL_NUMBER",
        )
    except TypeError:
        print("ℹ️ gspread antigo → fallback sem parâmetros de renderização.")
        header_rows = ws_src.get(f"{COL_INICIO}3:{COL_FIM}3")
    header = header_rows[0] if header_rows else []
    total_cols = len(header) if header else 0

    print("📥 Lendo dados (A4:AN) como valores nativos…")
    try:
        data = ws_src.get(
            f"{COL_INICIO}4:{COL_FIM}",
            value_render_option="UNFORMATTED_VALUE",
            date_time_render_option="SERIAL_NUMBER",
        )
    except TypeError:
        data = ws_src.get(f"{COL_INICIO}4:{COL_FIM}")

    # Remove linhas 100% vazias ao final
    while data and all((c == "" or c is None) for c in data[-1]):
        data.pop()

    if header:
        total_cols = len(header)
        data = normalize_width(data, total_cols)

    t_read1 = time.time()
    print(f"🔎 Linhas lidas: {len(data)} (sem contar cabeçalho) | Colunas: {total_cols} | ⏱️ leitura: {t_read1 - t_read0:.2f}s")

    if not header and not data:
        print("⚠️ Nada para copiar. Limpando destino e finalizando com timestamp.")
        ws_dst.batch_clear([f"{COL_INICIO}:{COL_FIM}"])
        set_status(ws_dst, datetime.now().strftime("Atualizado em: %d/%m/%Y %H:%M:%S"))
        print(f"🟢 Concluído (sem dados). ⏱️ total: {time.time() - t0:.2f}s")
        return

    # -------- LIMPEZA DESTINO --------
    t_clear0 = time.time()
    print("🧹 Limpando destino (A:AN)…")
    try:
        ws_dst.batch_clear([f"{COL_INICIO}:{COL_FIM}"])
    except APIError as e:
        print(f"⚠️ batch_clear falhou: {e}. Tentando clear() geral…")
        ws_dst.clear()
    t_clear1 = time.time()
    print(f"✅ Limpeza concluída. ⏱️ {t_clear1 - t_clear0:.2f}s")

    # -------- ESCRITA --------
    if header:
        print("✍️ Gravando cabeçalho em A2…")
        ws_dst.update(a1_range(COL_INICIO, 2, COL_FIM, 2), [header], raw=True)

    if data:
        total_rows = len(data)
        print(f"🚚 Gravando {total_rows} linhas em blocos de {CHUNK_ROWS}…")
        start = 0
        row_cursor = 3
        est_start = time.time()
        while start < total_rows:
            chunk = data[start:start + CHUNK_ROWS]
            end_row = row_cursor + len(chunk) - 1
            t_b0 = time.time()
            ws_dst.update(a1_range(COL_INICIO, row_cursor, COL_FIM, end_row), chunk, raw=True)
            t_b1 = time.time()
            print(f"   • Gravado {row_cursor}-{end_row} ({len(chunk)} linhas) | ⏱️ {t_b1 - t_b0:.2f}s")

            start += CHUNK_ROWS
            row_cursor = end_row + 1

            done = min(start, total_rows)
            elapsed = time.time() - est_start
            rate = done/elapsed if elapsed > 0 else 0
            remaining = (total_rows - done)/rate if rate > 0 else 0
            print(f"     Progresso: {done}/{total_rows} | Velocidade: {rate:.1f} l/s | ETA ~ {remaining:.1f}s")

    # -------- TIMESTAMP --------
    set_status(ws_dst, datetime.now().strftime("Atualizado em: %d/%m/%Y %H:%M:%S"))
    print(f"\n🟢 Concluído. ⏱️ total: {time.time() - t0:.2f}s")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ ERRO FATAL:")
        traceback.print_exc()

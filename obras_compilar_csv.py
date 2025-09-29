# -*- coding: utf-8 -*-
"""
Gera dois CSVs na mesma pasta do Drive (com cabeçalho e separador ';'):
1) Historico_Diario.csv  -> concatena todas as linhas de todos os arquivos MM-YYYY
2) Historico_Mensal.csv  -> pega somente as linhas da última data (coluna A) de cada arquivo MM-YYYY

Leitura robusta (CSV/Excel/Google Sheets), suporte a Shared Drives e atalhos.
"""

import io
import re
import sys
import csv
from typing import List, Tuple, Optional
import pandas as pd

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.errors import HttpError
import gspread

# ============== CONFIG ==============
FOLDER_ID = "1108v_R_-KpYXclfUPaXsRqzsyQ0tiMjh"
SERVICE_ACCOUNT_FILE = "credenciais.json"

# Se quiser forçar uma aba específica nos Google Sheets (ex.: "Base")
GOOGLE_SHEET_TAB_NAME: Optional[str] = None

OUTPUT_DAILY_NAME = "Historico_Diario.csv"
OUTPUT_MONTHLY_NAME = "Historico_Mensal.csv"

# CSV de saída: separador ';' e BOM para abrir bonito no Excel
CSV_SEPARATOR = ";"
CSV_ENCODING = "utf-8-sig"   # adiciona BOM
CSV_LINE_TERMINATOR = "\n"   # Excel aceita bem \n
CSV_QUOTING = csv.QUOTE_MINIMAL
# ====================================

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# Aceita "MM-YYYY" com ou sem extensão/espacos (ex.: "03-2025", "03-2025.csv", "03-2025 .xlsx")
MONTH_FILE_REGEX = re.compile(r"^\s*\d{2}-\d{4}\s*(?:\.[A-Za-z0-9]+)?\s*$")


def auth_clients():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    gc = gspread.authorize(creds)
    return drive, gc


def list_month_files(drive) -> List[Tuple[str, str, str]]:
    page_token = None
    results = []
    all_names_debug = []

    while True:
        resp = drive.files().list(
            q=f"'{FOLDER_ID}' in parents and trashed = false",
            fields=("nextPageToken, files(id, name, mimeType, "
                    "shortcutDetails(targetId, targetMimeType))"),
            pageSize=1000,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora="allDrives",
        ).execute()

        for f in resp.get("files", []):
            name = (f.get("name") or "").strip()
            all_names_debug.append(name)
            mime = f.get("mimeType")
            fid = f.get("id")

            # Resolve atalhos
            if mime == "application/vnd.google-apps.shortcut":
                sd = f.get("shortcutDetails") or {}
                target_id = sd.get("targetId")
                target_mime = sd.get("targetMimeType")
                if target_id and target_mime:
                    fid = target_id
                    mime = target_mime

            if MONTH_FILE_REGEX.match(name):
                results.append((name, fid, mime))

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    print("📝 Arquivos encontrados (todos):")
    for nm in sorted(all_names_debug):
        print("   •", nm)
    print("\n📝 Arquivos que casaram com MM-YYYY:")
    for nm, _, _ in sorted(results):
        print("   ✓", nm)
    print()
    return results


def download_drive_file_bytes(drive, file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=2 * 1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def export_google_sheet_as_csv(drive, file_id: str) -> bytes:
    request = drive.files().export_media(fileId=file_id, mimeType="text/csv")
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=2 * 1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def read_google_sheet_to_df(gc, file_id: str) -> pd.DataFrame:
    sh = gc.open_by_key(file_id)
    ws = sh.worksheet(GOOGLE_SHEET_TAB_NAME) if GOOGLE_SHEET_TAB_NAME else sh.get_worksheet(0)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header, rows = values[0], values[1:]
    return pd.DataFrame(rows, columns=header if header else None)


def load_month_file_to_df(drive, gc, name: str, file_id: str, mime: str) -> pd.DataFrame:
    try:
        if mime == "application/vnd.google-apps.spreadsheet":
            if GOOGLE_SHEET_TAB_NAME:
                df = read_google_sheet_to_df(gc, file_id)
            else:
                content = export_google_sheet_as_csv(drive, file_id)
                df = pd.read_csv(io.BytesIO(content), dtype=str)
        elif mime in (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
        ):
            content = download_drive_file_bytes(drive, file_id)
            df = pd.read_excel(io.BytesIO(content), dtype=str)
        else:
            # CSV no Drive costuma ser text/csv ou text/plain (às vezes application/octet-stream)
            content = download_drive_file_bytes(drive, file_id)
            try:
                df = pd.read_csv(io.BytesIO(content), dtype=str)
            except Exception:
                df = pd.read_csv(io.BytesIO(content), dtype=str, sep=";")

        if df.empty:
            print(f"⚠️  '{name}' vazio.")
            return pd.DataFrame()

        df.columns = [str(c).strip() for c in df.columns]
        df["__ARQUIVO_ORIGEM__"] = name
        df["__FILE_ID__"] = file_id
        return df

    except Exception as e:
        print(f"❌ Erro ao ler '{name}' ({file_id}): {e}")
        return pd.DataFrame()


def ensure_first_col_datetime(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    first_col = df.columns[0]  # coluna A é a data
    df["__DATA_COL_A__"] = pd.to_datetime(df[first_col], dayfirst=True, errors="coerce")
    return df


def build_daily_and_monthly(dfs: List[pd.DataFrame]):
    if not dfs:
        return pd.DataFrame(), pd.DataFrame()

    daily_df = pd.concat(dfs, ignore_index=True, copy=False)
    daily_df = ensure_first_col_datetime(daily_df)

    if daily_df.empty or "__ARQUIVO_ORIGEM__" not in daily_df.columns or "__DATA_COL_A__" not in daily_df.columns:
        return daily_df, pd.DataFrame()

    monthly_parts = []
    for origem, grupo in daily_df.groupby("__ARQUIVO_ORIGEM__", dropna=False):
        max_date = grupo["__DATA_COL_A__"].max()
        if pd.isna(max_date):
            continue
        monthly_parts.append(grupo[grupo["__DATA_COL_A__"] == max_date])

    monthly_df = pd.concat(monthly_parts, ignore_index=True) if monthly_parts else pd.DataFrame()
    return daily_df, monthly_df


def delete_if_exists(drive, filename: str):
    """Remove arquivos com mesmo nome; robusto para Shared Drives (404/403)."""
    resp = drive.files().list(
        q=f"name = '{filename}' and '{FOLDER_ID}' in parents and trashed = false",
        fields="files(id, name)",
        pageSize=100,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
    ).execute()

    for f in resp.get("files", []):
        fid = f["id"]
        try:
            # 1) tenta excluir direto
            drive.files().delete(fileId=fid, supportsAllDrives=True).execute()
            print(f"🧹 Apagado arquivo antigo: {f['name']} ({fid})")
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status in (403, 404):
                # 2) fallback: mover para lixeira
                try:
                    drive.files().update(
                        fileId=fid,
                        body={"trashed": True},
                        supportsAllDrives=True,
                    ).execute()
                    print(f"🗑️  Movido para lixeira: {f['name']} ({fid})")
                except Exception as e2:
                    # 3) não bloquear fluxo
                    print(f"⚠️  Não foi possível excluir/lixeirar {f['name']} ({fid}): {e2}")
            else:
                print(f"⚠️  Erro ao excluir {f['name']} ({fid}): {e}")


def upload_csv_to_drive(drive, df: pd.DataFrame, filename: str):
    if df is None or df.empty:
        print(f"⚠️  '{filename}' está vazio; não será enviado.")
        return

    # remove colunas auxiliares antes de salvar
    if "__DATA_COL_A__" in df.columns:
        df = df.drop(columns=["__DATA_COL_A__"])

    # grava CSV local com separador ';', cabeçalhos e BOM
    df.to_csv(
        filename,
        index=False,
        sep=CSV_SEPARATOR,
        encoding=CSV_ENCODING,
        lineterminator=CSV_LINE_TERMINATOR,
        quoting=CSV_QUOTING,
    )

    # apaga anterior e envia novo
    delete_if_exists(drive, filename)

    media = MediaFileUpload(filename, mimetype="text/csv", resumable=False)
    meta = {"name": filename, "parents": [FOLDER_ID], "mimeType": "text/csv"}
    created = drive.files().create(
        body=meta,
        media_body=media,
        fields="id,name",
        supportsAllDrives=True,  # necessário em Drives Compartilhados
    ).execute()
    print(f"✅ Enviado: {filename} (id: {created['id']})")


def main():
    print("🔐 Autenticando...")
    drive, gc = auth_clients()
    print("✅ Autenticado.\n")

    print("🔎 Listando arquivos MM-YYYY na pasta...")
    month_files = list_month_files(drive)
    if not month_files:
        print("⚠️  Nenhum arquivo no formato MM-YYYY encontrado na pasta.")
        sys.exit(0)

    dfs = []
    for name, fid, mime in month_files:
        print(f"📥 Lendo '{name}' ({mime}) ...")
        df = load_month_file_to_df(drive, gc, name, fid, mime)
        if df.empty:
            print(f"   ⚠️  '{name}' sem dados, ignorado.\n")
            continue

        df = ensure_first_col_datetime(df)
        if "__DATA_COL_A__" in df.columns and df["__DATA_COL_A__"].notna().any():
            maxd = df["__DATA_COL_A__"].max()
            print(f"   ↳ Última data encontrada: {maxd.strftime('%d/%m/%Y')}")
        print("   ✅ Ok.\n")
        dfs.append(df)

    print("🧮 Construindo bases...")
    daily_df, monthly_df = build_daily_and_monthly(dfs)
    print(f"   • Historico_Diario: {len(daily_df)} linhas")
    print(f"   • Historico_Mensal: {len(monthly_df)} linhas\n")

    print("📤 Enviando CSVs para a pasta do Drive (separador ';')...")
    upload_csv_to_drive(drive, daily_df, OUTPUT_DAILY_NAME)
    upload_csv_to_drive(drive, monthly_df, OUTPUT_MONTHLY_NAME)
    print("\n🎉 Concluído!")


if __name__ == "__main__":
    # Dependências:
    #   pip install google-api-python-client google-auth gspread pandas
    # Observações:
    #   - Compartilhe a pasta do Drive com o e-mail do client_email do credenciais.json
    #   - Se seus arquivos de mês forem Google Sheets com aba específica, defina GOOGLE_SHEET_TAB_NAME
    main()

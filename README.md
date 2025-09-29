# OEA Pipeline

Pipeline que:
1. Varre a pasta do Drive com arquivos `MM-YYYY`, consolida e publica:
   - `Historico_Diario.csv`
   - `Historico_Mensal.csv`  ➜ 📁 Drive (mesma pasta)
2. Importa `Historico_Mensal.csv` para **BD_Mensal** (A:AK), convertendo **apenas**
   - Datas: A, D, AK
   - Números: E, L..Y
3. Replica **BD_Carteira (A:AN, a partir da linha 3)** para **Base_Esteira** (A2), preservando tipos nativos.

## Scripts
- `atualizar_oea.py` — orquestra e reloga cada etapa, com 3 tentativas e logs em `logs/`. :contentReference[oaicite:6]{index=6}
- `obras_compilar_csv.py` — lê arquivos `MM-YYYY` na pasta do Drive (Shared/My Drive, com atalhos), gera e publica os CSVs. :contentReference[oaicite:7]{index=7}
- `replicar_bd_mensal.py` — baixa `Historico_Mensal.csv` da pasta e cola em **BD_Mensal** (A:AK), tratando colunas seletivas. :contentReference[oaicite:8]{index=8}
- `replicar_esteira_oea.py` — copia **BD_Carteira ➜ Base_Esteira** lendo valores nativos (sem apóstrofos), em blocos. :contentReference[oaicite:9]{index=9}

## Pré-requisitos (local)
- Python 3.11+
- Um `credenciais.json` de **Service Account** com acesso às planilhas e à pasta do Drive.

## Rodar localmente
```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# coloque o credenciais.json na raiz
python atualizar_oea.py

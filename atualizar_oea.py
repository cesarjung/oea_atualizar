# atualizar_oea.py  — orquestrador verboso com logs por etapa (UTF-8 fix)
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPTS = [
    "obras_compilar_csv.py",
    "replicar_esteira_oea.py",
    "replicar_bd_mensal.py",
]

RETRIES_PER_STEP = 3
BASE_SLEEP = 5  # segundos

PYTHON_EXE_CANDIDATES = [
    sys.executable,
    str(Path("venv/Scripts/python.exe")),
    str(Path(".venv/Scripts/python.exe")),
    "python",
    "python3",
]

BANNER = "🚀 OEA Pipeline"
LINE = "—" * 64

# Ambiente do filho: força UTF-8 e saída sem buffer
ENV = os.environ.copy()
ENV["PYTHONUTF8"] = "1"
ENV["PYTHONIOENCODING"] = "utf-8"
ENV["PYTHONUNBUFFERED"] = "1"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

def find_python():
    for exe in PYTHON_EXE_CANDIDATES:
        try:
            subprocess.run([exe, "--version"], capture_output=True, check=True)
            return exe
        except Exception:
            continue
    print("❌ Nenhum interpretador Python válido encontrado.")
    sys.exit(1)

def tail_text(text: str, n_lines: int = 80) -> str:
    lines = text.splitlines()
    return "\n".join(lines[-n_lines:]) if len(lines) > n_lines else text

def run_step(python_exe: str, script_path: str) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"{Path(script_path).stem}_{ts}.log"
    cmd = [python_exe, "-u", "-X", "utf8", script_path]  # filho em UTF-8

    print(f"\n{LINE}\n▶️  Rodando: {script_path}")
    print(f"   • Python: {python_exe}")
    print(f"   • CWD   : {Path.cwd()}")
    print(f"   • CMD   : {' '.join(cmd)}")
    print(f"   • Log   : {log_file}")

    for attempt in range(1, RETRIES_PER_STEP + 1):
        print(f"   • Tentativa {attempt}/{RETRIES_PER_STEP} …")
        start = time.time()
        with open(log_file, "a", encoding="utf-8", newline="") as lf:
            lf.write(f"\n===== {datetime.now():%Y-%m-%d %H:%M:%S} :: START {script_path} =====\n")
            lf.flush()
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",          # <<< DECODIFICA UTF-8
                    errors="replace",          # <<< NÃO QUEBRA se vier lixo
                    env=ENV,
                )
                assert proc.stdout is not None
                for line in proc.stdout:
                    print(line.rstrip())
                    lf.write(line)
                rc = proc.wait()
                lf.write(f"===== END (rc={rc}) =====\n")
            except Exception as e:
                lf.write(f"===== EXCEPTION: {e} =====\n")
                rc = 1

        elapsed = time.time() - start
        if rc == 0:
            print(f"✅ Sucesso: {script_path}  ({elapsed:.1f}s)")
            return

        # Falhou — diagnóstico rápido
        try:
            log_text = log_file.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            log_text = ""
        print(f"❌ {script_path} falhou (rc={rc}) em {elapsed:.1f}s.")
        if log_text.strip():
            print("---- Fim do log (últimas 80 linhas) ----")
            print(tail_text(log_text, 80))
            print("---- (veja o arquivo completo no diretório logs) ----")
        else:
            print("⚠️  O script não gerou saída. Verifique dependências, caminhos e permissões.")

        if attempt < RETRIES_PER_STEP:
            sleep_s = BASE_SLEEP * attempt
            print(f"⚠️  Re-tentando em {sleep_s}s…")
            time.sleep(sleep_s)
        else:
            raise SystemExit(1)

def main():
    print(LINE)
    print(f"{BANNER} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(LINE)

    python_exe = find_python()

    missing = [s for s in SCRIPTS if not Path(s).exists()]
    if missing:
        print("❌ Arquivos não encontrados:", ", ".join(missing))
        sys.exit(1)

    for script in SCRIPTS:
        run_step(python_exe, script)

    print(f"\n🎉 Pipeline concluído com sucesso! ({datetime.now().strftime('%H:%M:%S')})")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário.")
        sys.exit(130)
    except SystemExit as e:
        sys.exit(int(str(e) or 1))
    except Exception:
        sys.exit(1)

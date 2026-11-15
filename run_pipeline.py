import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv

env_path = Path.home() / ".secrets" / "startup_informer.env"
loaded = load_dotenv(env_path, override=True, encoding="utf-8-sig")
print(f"ENV loaded={loaded} from {env_path} (exists={env_path.exists()})")


def run_step(script_path: str, args: list[str] | None = None, cwd: str | None = None) -> None:
    cmd = [sys.executable, script_path, *(args or [])]
    print(f"\n>>> Запуск: {' '.join(cmd)} (cwd={cwd or '.'})")
    subprocess.run(cmd, cwd=cwd, check=True)


def main(search_term: str | None = None, channel_name: str = "Alina") -> None:

    if not os.getenv("TELEGRAM_API_ID") or not os.getenv("TELEGRAM_API_HASH"):
        raise RuntimeError("Відсутні TELEGRAM_API_ID або TELEGRAM_API_HASH у змінних середовища.")

    # Етап 1: Вивантаження з Telegram у папку dialogs/
    if search_term:
        run_step("ingestion/parse_history.py", args=[search_term])
    else:
        os.environ["SPECIFIC_DIALOG"] = channel_name
        run_step("ingestion/parse_concrete_channel_history.py")

    # Етап 2: Конвертація dialogs/ -> message_table/ та user_table/ (Delta)
    # Запускаємо з cwd="pyspark_jobs", бо всередині зашиті відносні шляхи ../dialogs/
    run_step("create_delta_tables.py", cwd="pyspark_jobs")


if __name__ == "__main__":
    # Передай search_term="AI" для пошуку по всіх чатах або залиш None для конкретного каналу
    main(search_term=None, channel_name="Аліна Дяченко")
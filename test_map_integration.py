#!/usr/bin/env python3
# Проверка интеграции с сервисом карт (заявки на опустошение — fillLevel=100)

import os
from pathlib import Path

# Загрузка .env вручную (без зависимости dotenv)
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\"").lstrip())

import json
import urllib.request
import urllib.error

BASE = os.environ.get("MAP_SERVICE_URL", "https://map.blagokirov.ru").rstrip("/")


def main():
    if not BASE:
        print("Ошибка: MAP_SERVICE_URL не задан в .env")
        exit(1)

    print(f"MAP_SERVICE_URL: {BASE}")
    print()

    # 1. GET — загрузка бункеров
    print("1. GET /api/bunkers ...")
    try:
        req = urllib.request.Request(f"{BASE}/api/bunkers")
        with urllib.request.urlopen(req, timeout=10) as r:
            bunkers = json.loads(r.read().decode())
        print(f"   Статус: 200")
        print(f"   Загружено бункеров: {len(bunkers)}")
        if not bunkers:
            print("   Нет бункеров для теста.")
            exit(0)
    except urllib.error.HTTPError as e:
        print(f"   Статус: {e.code}")
        print(f"   Ответ: {(e.read().decode() or '')[:300]}")
        exit(1)
    except Exception as e:
        print(f"   Ошибка: {e}")
        exit(1)

    # 2. PUT — обновление fillLevel (тестовый бункер)
    first = bunkers[0]
    bid = first.get("id")
    old_fill = first.get("fillLevel", 0)
    api_key = os.environ.get("MAP_BOT_API_KEY", "").strip()
    headers = {"Content-Type": "application/json", "X-API-Key": api_key} if api_key else {"Content-Type": "application/json"}

    print(f"\n2. PUT /api/bunkers/{bid} (fillLevel=100) ...")
    try:
        body = json.dumps({"fillLevel": 100}).encode()
        req = urllib.request.Request(
            f"{BASE}/api/bunkers/{bid}",
            data=body,
            method="PUT",
            headers=headers,
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
        print(f"   Статус: 200")
        print(f"   fillLevel после PUT: {data.get('fillLevel')}")
    except urllib.error.HTTPError as e:
        print(f"   Статус: {e.code}")
        body = e.read().decode()
        print(f"   Ответ: {body[:300]}")
        if e.code == 401:
            print("\n   Возможная причина: API карты требует авторизацию.")
            print("   Убедитесь, что MAP_BOT_API_KEY в .env карты совпадает с ключом в боте.")
        exit(1)
    except Exception as e:
        print(f"   Ошибка: {e}")
        exit(1)

    # 3. Восстановить исходное значение
    print(f"\n3. Восстановление fillLevel={old_fill} ...")
    try:
        req = urllib.request.Request(
            f"{BASE}/api/bunkers/{bid}",
            data=json.dumps({"fillLevel": old_fill}).encode(),
            method="PUT",
            headers=headers,
        )
        urllib.request.urlopen(req, timeout=10)
        print("   Восстановлено.")
    except Exception as e:
        print(f"   Предупреждение: не удалось восстановить: {e}")

    print("\nOK: Интеграция с картой работает.")


if __name__ == "__main__":
    main()

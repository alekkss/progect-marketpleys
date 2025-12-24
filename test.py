# test_proxy_simple.py
import httpx

PROXY = "http://3HTmqd:PytrZC@193.32.153.99:9950"

print(f"[*] Тестирую прокси: {PROXY}\n")

try:
    # Проверка 1: IP адрес
    client = httpx.Client(proxy=PROXY, timeout=30.0)
    
    resp = client.get("https://api.ipify.org?format=json")
    ip = resp.json()['ip']
    print(f"[✓] IP через прокси: {ip}")
    
    # Проверка 2: Геолокация
    resp = client.get(f"http://ip-api.com/json/{ip}")
    geo = resp.json()
    print(f"[✓] Страна: {geo.get('country')} ({geo.get('countryCode')})")
    print(f"[✓] Город: {geo.get('city')}")
    
    # Проверка 3: OpenRouter
    headers = {"Authorization": "Bearer your-key-here"}  # Необязательно для теста
    resp = client.get("https://openrouter.ai/api/v1/models", headers=headers)
    print(f"\n[!] OpenRouter ответ: {resp.status_code}")
    
    if resp.status_code == 403:
        print("[✗] 403 - Прокси из заблокированного региона!")
    elif resp.status_code == 200:
        print("[✓] 200 - OpenRouter доступен!")
    
    client.close()
    
except Exception as e:
    print(f"[✗] Ошибка: {e}")
    import traceback
    traceback.print_exc()

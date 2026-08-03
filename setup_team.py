import asyncio
import aiohttp

# Конфигурация API
BASE_URL = "https://integrate.api.nvidia.com/v1"
API_KEYS = {
    "main": "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",
    "secondary": "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"
}

# Команда агентов
TEAM = [
    {"name": "boss", "role": "manager", "model": "meta/llama-3.1-8b-instruct", "key": API_KEYS["main"]},
    {"name": "dev", "role": "coder", "model": "nvidia/nemotron-3-nano-30b-a3b", "key": API_KEYS["secondary"]},
    {"name": "researcher", "role": "web_search", "model": "meta/llama-3.1-8b-instruct", "key": API_KEYS["main"]},
    {"name": "analyst", "role": "data_analysis", "model": "nvidia/nemotron-3-nano-30b-a3b", "key": API_KEYS["secondary"]}
]

async def register_agent(session, agent):
    url = "http://127.0.0.1:5000/api/agents"
    payload = {
        "name": agent["name"],
        "api_key": agent["key"],
        "base_url": BASE_URL,
        "model": agent["model"],
        "role": agent["role"] # Передаем роль для системного промпта
    }
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                print(f"✅ Агент [{agent['name']}] ({agent['role']}) зарегистрирован.")
            else:
                text = await resp.text()
                print(f"❌ Ошибка регистрации [{agent['name']}]: {text}")
    except Exception as e:
        print(f"⚠️ Исключение для [{agent['name']}]: {e}")

async def main():
    print("🚀 Инициализация мульти-агентной команды...")
    # Небольшая пауза, чтобы сервер точно успел стартовать
    await asyncio.sleep(2)
    
    async with aiohttp.ClientSession() as session:
        tasks = [register_agent(session, agent) for agent in TEAM]
        await asyncio.gather(*tasks)
    
    print("\n🎉 Команда сформирована! Босс готов управлять.")

if __name__ == "__main__":
    asyncio.run(main())

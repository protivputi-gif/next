import asyncio
import aiohttp
import json

async def send_task(task_description):
    url = "http://127.0.0.1:5000/api/chat/stream"
    payload = {"message": task_description, "image": None}
    
    print(f"\n📤 ЗАДАЧА БОССУ: {task_description}")
    print("=" * 60)
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                print(f"Ошибка HTTP: {resp.status}")
                return

            async for line in resp.content:
                line = line.decode('utf-8').strip()
                if not line or line.startswith(':'):
                    continue
                
                if line.startswith('event:'):
                    event_type = line.split(':', 1)[1].strip()
                    data_line = await resp.content.readline()
                    data_str = data_line.decode('utf-8').strip()
                    if data_str.startswith('data:'):
                        data_str = data_str[5:].strip()
                    
                    try:
                        data = json.loads(data_str)
                        
                        if event_type == 'delegation':
                            worker = data.get('worker', 'unknown')
                            task = data.get('task', '')
                            print(f"\n🔄 [ДЕЛЕГИРОВАНИЕ] БОСС → {worker.upper()}")
                            print(f"   Задача: {task}")
                        elif event_type == 'action':
                            content = data.get('content', '')
                            if 'Result:' in content:
                                print(f"   👁️ Результат инструмента: {content[:150]}...")
                        elif event_type == 'result':
                            result = data.get('content', '')
                            print(f"\n✅ [ФИНАЛЬНЫЙ ОТВЕТ ОТ {event_type.upper()}]:\n{result}")
                            print("=" * 60)
                            return result
                        elif event_type == 'error':
                            print(f"❌ Ошибка: {data.get('content', '')}")
                            return
                    except json.JSONDecodeError:
                        pass

async def main():
    # Тест 1: Код (должен делегировать dev)
    await send_task("Напиши функцию Python для расчёта чисел Фибоначчи до n=10 и выполни её")
    
    await asyncio.sleep(2)
    
    # Тест 2: Анализ данных (должен делегировать analyst)
    await send_task("Проанализируй данные [10, 20, 30, 40, 50]: найди среднее, медиану и стандартное отклонение")
    
    await asyncio.sleep(2)
    
    # Тест 3: Поиск в вебе (должен делегировать researcher)
    await send_task("Найди последнюю информацию о ценах на NVIDIA акции через fetch_url")

if __name__ == "__main__":
    asyncio.run(main())

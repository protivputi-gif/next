import asyncio
import aiohttp
import json

async def send_task(task_description):
    url = "http://127.0.0.1:5000/api/chat/stream"
    payload = {
        "message": task_description,
        "image": None
    }
    
    print(f"\n📤 Отправка задачи Боссу: '{task_description}'")
    print("-" * 50)
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    print(f"Ошибка HTTP: {resp.status}")
                    return

                # Чтение SSE потока
                async for line in resp.content:
                    line = line.decode('utf-8').strip()
                    if not line or line.startswith(':'):
                        continue
                    
                    if line.startswith('event:'):
                        event_type = line.split(':', 1)[1].strip()
                        # Следующая строка - данные
                        data_line = await resp.content.readline()
                        data_str = data_line.decode('utf-8').strip()
                        
                        if data_str.startswith('data:'):
                            data_str = data_str[5:].strip()
                        
                        try:
                            data = json.loads(data_str)
                            
                            if event_type == 'thought':
                                print(f"💭 [БОСС Думает]: {data.get('content', '')[:100]}...")
                            elif event_type == 'delegation':
                                worker = data.get('worker', 'unknown')
                                task = data.get('task', '')
                                print(f"🔄 [ДЕЛЕГИРОВАНИЕ] -> {worker}: {task}")
                            elif event_type == 'action':
                                tool = data.get('tool', '')
                                inp = data.get('input', '')
                                print(f"⚙️ [{data.get('agent', 'BOSS')}] Инструмент: {tool}({inp[:50]}...)")
                            elif event_type == 'observation':
                                out = data.get('output', '')
                                print(f"👁️ Результат: {out[:150]}...")
                            elif event_type == 'final_answer':
                                print(f"\n✅ [ФИНАЛЬНЫЙ ОТВЕТ]:\n{data.get('response', '')}")
                                print("-" * 50)
                                return data.get('response')
                            elif event_type == 'error':
                                print(f"❌ Ошибка: {data.get('error', '')}")
                                return
                        except json.JSONDecodeError:
                            pass
                            
        except Exception as e:
            print(f"Исключение: {e}")

async def main():
    # Задача 1: Написание кода (должен делегировать dev)
    await send_task("Напиши функцию на Python для вычисления чисел Фибоначчи до n.")
    
    await asyncio.sleep(2)
    
    # Задача 2: Анализ данных (должен делегировать analyst)
    await send_task("Проанализируй список чисел [10, 20, 30, 40, 50] и найди среднее значение и медиану.")

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import aiohttp

API_URL = 'http://127.0.0.1:5000'

async def test_api():
    async with aiohttp.ClientSession() as session:
        # 1. Register Boss Agent
        boss_data = {
            'name': 'boss',
            'api_key': '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',
            'base_url': 'https://integrate.api.nvidia.com/v1',
            'model': 'meta/llama-3.1-8b-instruct'
        }
        
        print('=== Registering Boss Agent ===')
        async with session.post(f'{API_URL}/api/agents', json=boss_data) as resp:
            print(f'Status: {resp.status}')
            result = await resp.json()
            print(f'Response: {result}')
        
        # 2. Register Worker Agent
        worker_data = {
            'name': 'coder',
            'api_key': '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',
            'base_url': 'https://integrate.api.nvidia.com/v1',
            'model': 'nvidia/nemotron-3-nano-30b-a3b'
        }
        
        print('\n=== Registering Worker Agent ===')
        async with session.post(f'{API_URL}/api/agents', json=worker_data) as resp:
            print(f'Status: {resp.status}')
            result = await resp.json()
            print(f'Response: {result}')
        
        # 3. List Agents
        print('\n=== Listing Agents ===')
        async with session.get(f'{API_URL}/api/agents') as resp:
            print(f'Status: {resp.status}')
            result = await resp.json()
            print(f'Agents: {result}')
        
        # 4. Test Simple Chat with Boss
        print('\n=== Testing Simple Chat with Boss ===')
        chat_data = {
            'agent_name': 'boss',
            'message': 'Привет! Кто ты и какие у тебя есть помощники?'
        }
        async with session.post(f'{API_URL}/api/chat', json=chat_data, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            print(f'Status: {resp.status}')
            if resp.status == 200:
                result = await resp.json()
                print(f'Response: {result.get("response", "No response")[:300]}...')
            else:
                error = await resp.text()
                print(f'Error: {error}')
        
        # 5. Test Complex Task (Delegation)
        print('\n=== Testing Complex Task (Delegation) ===')
        complex_task = {
            'agent_name': 'boss',
            'message': 'Напиши Python скрипт, который считает сумму чисел от 1 до 100, выполни его и покажи результат.'
        }
        async with session.post(f'{API_URL}/api/chat/stream', json=complex_task, timeout=aiohttp.ClientTimeout(total=120)) as resp:
            print(f'Status: {resp.status}')
            if resp.status == 200:
                print('Streaming response:')
                async for line in resp.content:
                    line = line.decode('utf-8').strip()
                    if line:
                        print(line)
            else:
                error = await resp.text()
                print(f'Error: {error}')

if __name__ == '__main__':
    asyncio.run(test_api())

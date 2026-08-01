import os
import sys
import json
import time
import hashlib
import asyncio
import subprocess
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict, deque

# --- НАСТРОЙКА ЛОГИРОВАНИЯ ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nexus_agent.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("NexusCore")

try:
    import aiohttp
except ImportError:
    logger.info("Установка aiohttp...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "aiohttp"])
    import aiohttp

try:
    import aiosqlite
except ImportError:
    logger.info("Установка aiosqlite...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "aiosqlite"])
    import aiosqlite

try:
    from quart import Quart, request, jsonify, render_template_string
except ImportError:
    logger.info("Установка Quart...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "quart"])
    from quart import Quart, request, jsonify, render_template_string

# --- КОНФИГУРАЦИЯ ---
DB_PATH = "nexus_memory.db"
app = Quart(__name__)

# --- ЛЕГКОВЕСНАЯ ГРАФОВАЯ ПАМЯТЬ ---
class GraphNode:
    """Узел графа знаний"""
    def __init__(self, node_id: str, label: str, node_type: str = "concept"):
        self.id = node_id
        self.label = label
        self.type = node_type  # concept, entity, error, tool, file, lesson
        self.properties = {"created_at": time.time(), "access_count": 0}
        self.edges: Dict[str, List[str]] = defaultdict(list)  # relation -> [target_ids]

    def add_edge(self, relation: str, target_id: str):
        if target_id not in self.edges[relation]:
            self.edges[relation].append(target_id)

class GraphMemory:
    """
    Легковесная графовая память на основе SQLite + RAM.
    Оптимизирована для слабых ПК: граф хранится в RAM, персистентность в SQLite.
    """
    def __init__(self):
        self.nodes: Dict[str, GraphNode] = {}
        self._init_db()
        # _load_graph будет вызван асинхронно в init_db сервера
        logger.info("[GRAPH] Графовая память инициализирована (ожидание async загрузки).")

    def _init_db(self):
        # Инициализация БД будет выполнена асинхронно в _init_async
        pass

    async def _init_async(self):
        """Асинхронная инициализация базы данных и загрузка графа"""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS graph_nodes
                         (id TEXT PRIMARY KEY, label TEXT, type TEXT, properties TEXT)''')
            await db.execute('''CREATE TABLE IF NOT EXISTS graph_edges
                         (source_id TEXT, relation TEXT, target_id TEXT,
                          PRIMARY KEY (source_id, relation, target_id))''')
            await db.commit()
        await self._load_graph()
        logger.info("[GRAPH] База данных инициализирована и граф загружен (async).")

    def _get_node_id(self, label: str) -> str:
        return hashlib.md5(label.lower().strip().encode()).hexdigest()[:12]

    async def add_node(self, label: str, node_type: str = "concept") -> GraphNode:
        node_id = self._get_node_id(label)
        if node_id not in self.nodes:
            node = GraphNode(node_id, label, node_type)
            self.nodes[node_id] = node
            await self._save_node_async(node)
            logger.debug(f"[GRAPH] Добавлен узел: {label} ({node_type})")
        else:
            self.nodes[node_id].properties["access_count"] += 1
        return self.nodes[node_id]

    async def add_edge(self, source_label: str, target_label: str, relation: str = "related_to"):
        src = await self.add_node(source_label)
        tgt = await self.add_node(target_label)
        src.add_edge(relation, tgt.id)
        # Обратная связь для неиерархических отношений
        if relation not in ["causes", "requires", "uses"]:
            tgt.add_edge(f"reverse_{relation}", src.id)
        await self._save_edge_async(src.id, relation, tgt.id)
        logger.debug(f"[GRAPH] Связь: {source_label} --[{relation}]--> {target_label}")

    def get_neighbors(self, label: str, depth: int = 1) -> List[Dict]:
        """BFS поиск соседей для контекста"""
        start_id = self._get_node_id(label)
        if start_id not in self.nodes:
            return []
        
        visited = set()
        queue = deque([(start_id, 0)])
        results = []
        
        while queue:
            curr_id, dist = queue.popleft()
            if curr_id in visited or dist > depth:
                continue
            visited.add(curr_id)
            
            node = self.nodes.get(curr_id)
            if node and curr_id != start_id:
                results.append({
                    "label": node.label,
                    "type": node.type,
                    "relations": {k: [self.nodes[tid].label for tid in v if tid in self.nodes] 
                                  for k, v in node.edges.items()}
                })
            
            if dist < depth:
                for targets in node.edges.values():
                    for tid in targets:
                        if tid not in visited:
                            queue.append((tid, dist + 1))
        return results

    def _save_node(self, node: GraphNode):
        # Будет реализовано в асинхронной версии
        pass

    async def _save_node_async(self, node: GraphNode):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO graph_nodes (id, label, type, properties) VALUES (?, ?, ?, ?)",
                  (node.id, node.label, node.type, json.dumps(node.properties)))
            await db.commit()

    def _save_edge(self, src: str, rel: str, tgt: str):
        # Будет реализовано в асинхронной версии
        pass

    async def _save_edge_async(self, src: str, rel: str, tgt: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR IGNORE INTO graph_edges (source_id, relation, target_id) VALUES (?, ?, ?)",
                  (src, rel, tgt))
            await db.commit()

    async def _load_graph(self):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT id, label, type, properties FROM graph_nodes") as cursor:
                async for row in cursor:
                    node = GraphNode(row[0], row[1], row[2])
                    node.properties = json.loads(row[3])
                    self.nodes[row[0]] = node
            async with db.execute("SELECT source_id, relation, target_id FROM graph_edges") as cursor:
                async for row in cursor:
                    if row[0] in self.nodes:
                        self.nodes[row[0]].add_edge(row[1], row[2])
        logger.info(f"[GRAPH] Загружено {len(self.nodes)} узлов из базы.")

# --- СИСТЕМА ПАМЯТИ С ОБУЧЕНИЕМ ---
class AdvancedMemory:
    def __init__(self):
        self.short_term = []
        self.graph = GraphMemory()

    def add_short_term(self, role, content):
        self.short_term.append({"role": role, "content": content})
        if len(self.short_term) > 10:
            self.short_term.pop(0)

    async def save_episode(self, role, content, success=True):
        async with aiosqlite.connect(DB_PATH) as db:
            ts = time.time()
            await db.execute("INSERT INTO episodes (timestamp, role, content, success) VALUES (?, ?, ?, ?)",
                  (ts, role, content, success))
            await db.commit()
        self.add_short_term(role, content)
        
        # Обновление графа
        await self.graph.add_node(role, "agent_role")
        if "ошибка" in content.lower() or "error" in content.lower() or "fail" in content.lower():
            await self.graph.add_node("Error", "system_state")
            await self.graph.add_edge(role, "Error", "encountered")
        logger.info(f"[MEMORY] Эпизод сохранен: {role} | Успех: {success}")

    async def save_lesson(self, keyword, lesson_text):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT count FROM lessons WHERE keyword=?", (keyword,)) as cursor:
                row = await cursor.fetchone()
            if row:
                await db.execute("UPDATE lessons SET count=count+1 WHERE keyword=?", (keyword,))
                logger.info(f"[MEMORY] Обновлен урок: {keyword}")
            else:
                await db.execute("INSERT INTO lessons (keyword, lesson_text) VALUES (?, ?)", (keyword, lesson_text))
                logger.info(f"[MEMORY] Создан урок: {keyword}")
            await db.commit()
        
        # Сохранение в граф
        await self.graph.add_edge(keyword, lesson_text[:50], "has_lesson")

    async def get_lessons(self, keywords):
        lessons = []
        async with aiosqlite.connect(DB_PATH) as db:
            for kw in keywords:
                async with db.execute("SELECT lesson_text FROM lessons WHERE keyword LIKE ?", (f"%{kw}%",)) as cursor:
                    rows = await cursor.fetchall()
                    lessons.extend([r[0] for r in rows])
        return lessons

    async def get_context(self, task_keywords=None):
        context = "=== Краткосрочная память ===\n"
        for msg in self.short_term:
            context += f"{msg['role']}: {msg['content']}\n"

        # Графовый контекст
        if task_keywords:
            graph_ctx = "\n=== СВЯЗАННЫЕ ЗНАНИЯ (ГРАФ) ===\n"
            found = False
            for kw in task_keywords:
                neighbors = self.graph.get_neighbors(kw, depth=1)
                if neighbors:
                    found = True
                    graph_ctx += f"[Сущность: {kw}] Связи:\n"
                    for n in neighbors:
                        graph_ctx += f"  - {n['label']} ({n['type']})\n"
            if found:
                context += graph_ctx

        lessons = await self.get_lessons(task_keywords) if task_keywords else []
        if lessons:
            context += "\n=== ИЗВЛЕЧЕННЫЕ УРОКИ ===\n"
            for l in lessons:
                context += f"- {l}\n"

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT role, content, success FROM episodes ORDER BY timestamp DESC LIMIT 5") as cursor:
                rows = await cursor.fetchall()

        if rows:
            context += "\n=== ПОСЛЕДНЯЯ ИСТОРИЯ ===\n"
            for r in rows:
                status = "OK" if r[2] else "FAIL"
                context += f"[{status}] {r[0]}: {r[1][:50]}...\n"
        return context

memory = AdvancedMemory()

# --- ИНСТРУМЕНТЫ (TOOLS) С БЕЗОПАСНОСТЬЮ И УСТАНОВКОЙ ---

class SystemTools:
    @staticmethod
    async def run_python_code(code: str):
        logger.info(f"[TOOL] Выполнение Python кода (длина: {len(code)} симв.)")
        try:
            local_scope = {"__builtins__": __builtins__}
            exec(code, local_scope, local_scope)
            result = str(local_scope.get('result', 'Код выполнен'))
            logger.info(f"[TOOL] Python код выполнен успешно")
            return result
        except Exception as e:
            logger.error(f"[TOOL] Ошибка Python: {str(e)}")
            return f"Ошибка Python: {str(e)}"

    @staticmethod
    async def run_shell_command(cmd: str):
        logger.info(f"[TOOL] Выполнение команды: {cmd}")
        dangerous = ["rm -rf /", "mkfs", "dd if="]
        if any(d in cmd for d in dangerous):
            logger.warning(f"[TOOL] Отказано в выполнении опасной команды: {cmd}")
            return "Отказано в выполнении опасной команды."

        try:
            proc = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env={**os.environ, "PYTHONUNBUFFERED": "1"}
            )
            # В Python 3.12 communicate() не принимает timeout, используем wait_for + asyncio.wait_for
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            except asyncio.TimeoutError:
                # Принудительно завершаем процесс при таймауте
                try:
                    proc.kill()
                except:
                    pass
                logger.error("[TOOL] Таймаут выполнения команды")
                return "Таймаут выполнения команды (120 сек)."
            
            output = stdout.decode() + stderr.decode()
            logger.info(f"[TOOL] Команда выполнена, вывод: {output[:200]}...")
            return output
        except Exception as e:
            logger.error(f"[TOOL] Ошибка Shell: {str(e)}")
            return f"Ошибка Shell: {str(e)}"

    @staticmethod
    async def check_and_install(package_manager: str, package: str):
        """Проверяет и устанавливает пакеты (apt, pip, sdkmanager)"""
        logger.info(f"[TOOL] Установка пакета {package} через {package_manager}")
        cmd = ""
        if package_manager == "apt":
            cmd = f"apt-get update && apt-get install -y {package}"
        elif package_manager == "pip":
            cmd = f"pip install {package}"
        elif package_manager == "sdk":
            cmd = f"sdkmanager \"{package}\""
        else:
            logger.error(f"[TOOL] Неизвестный менеджер пакетов: {package_manager}")
            return f"Неизвестный менеджер пакетов: {package_manager}"

        return await SystemTools.run_shell_command(cmd)

    @staticmethod
    async def read_file(path: str):
        logger.info(f"[TOOL] Чтение файла: {path}")
        if not os.path.exists(path): 
            logger.warning(f"[TOOL] Файл не найден: {path}")
            return "Файл не найден."
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                logger.info(f"[TOOL] Файл прочитан ({len(content)} байт)")
                return content
        except Exception as e:
            logger.error(f"[TOOL] Ошибка чтения: {e}")
            return f"Ошибка чтения: {e}"

    @staticmethod
    async def write_file(path: str, content: str):
        logger.info(f"[TOOL] Запись файла: {path} ({len(content)} байт)")
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f"[TOOL] Файл успешно записан")
            return f"Файл {path} записан."
        except Exception as e:
            logger.error(f"[TOOL] Ошибка записи: {e}")
            return f"Ошибка записи: {e}"

    @staticmethod
    async def fetch_url(url: str):
        logger.info(f"[TOOL] Запрос URL: {url}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    text = await response.text()
                    logger.info(f"[TOOL] URL получен ({len(text)} байт)")
                    return text
        except Exception as e:
            logger.error(f"[TOOL] Ошибка сети: {e}")
            return f"Ошибка сети: {e}"

TOOLS = {
    "run_python": {"desc": "Выполнить Python код", "func": SystemTools.run_python_code},
    "run_shell": {"desc": "Выполнить команду оболочки (Linux/Mac)", "func": SystemTools.run_shell_command},
    "install_pkg": {"desc": "Установить пакет (apt/pip/sdk)", "func": SystemTools.check_and_install},
    "read_file": {"desc": "Прочитать файл", "func": SystemTools.read_file},
    "write_file": {"desc": "Записать файл", "func": SystemTools.write_file},
    "fetch_url": {"desc": "Получить содержимое URL", "func": SystemTools.fetch_url},
}

# --- ГРАФОВЫЙ ОРКЕСТРАТОР ---

class TaskNode:
    def __init__(self, node_id: int, description: str, assigned_agent: str = None):
        self.id = node_id
        self.description = description
        self.assigned_agent = assigned_agent
        self.dependencies: List[int] = []
        self.result: Optional[str] = None
        self.status = "pending"  # pending, running, done, failed

class GraphOrchestrator:
    def __init__(self, agents_registry):
        self.agents = agents_registry
        self.nodes: Dict[int, TaskNode] = {}
        self.next_id = 0

    def create_node(self, description, agent_name=None) -> int:
        node_id = self.next_id
        self.nodes[node_id] = TaskNode(node_id, description, agent_name)
        self.next_id += 1
        return node_id

    def add_dependency(self, from_node: int, to_node: int):
        if from_node in self.nodes and to_node in self.nodes:
            self.nodes[to_node].dependencies.append(from_node)

    async def execute_graph(self, main_task: str, planner_agent=None, image_data=None):
        """
        1. Планировщик разбивает задачу на граф.
        2. Выполняет узлы в топологическом порядке.
        3. Динамически перестраивает граф при ошибках.
        """
        # Шаг 1: Планирование
        plan_prompt = f"""
        Ты главный архитектор Nexus. Твоя задача: разбить задачу '{main_task}' на пошаговый план (граф).
        Доступные агенты: {list(self.agents.keys())}.
        Инструменты системы: {list(TOOLS.keys())}.

        Верни ТОЛЬКО JSON список шагов. Каждый шаг:
        {{
            "id": 0,
            "description": "Что сделать",
            "agent": "Имя агента или 'system_tool'",
            "depends_on": [список id предыдущих шагов]
        }}
        Если нужны системные команды (установка SDK, компиляция), используй агента 'System' или инструмент напрямую.
        """

        # Используем планировщика (или первого доступного агента как мета-агента)
        agent_list = list(self.agents.values())
        if not agent_list:
            return "Ошибка: Нет активных агентов для планирования."

        planner = planner_agent if planner_agent else agent_list[0] # Временное решение: первый агент как планировщик

        ctx = await memory.get_context(main_task.split())
        # Передаем изображение планировщику если есть
        
        logger.info("[ORCHESTRATOR] Запуск планировщика...")
        plan_result = await planner.think_and_act(plan_prompt, ctx, [], image_data)

        # Парсинг плана (упрощенный)
        try:
            # Очистка от markdown
            clean_json = re.search(r'\[.*\]', plan_result, re.DOTALL)
            if clean_json:
                plan_data = json.loads(clean_json.group())
            else:
                plan_data = json.loads(plan_result)

            logger.info(f"[ORCHESTRATOR] План получен: {len(plan_data)} шагов")
            
            # Построение графа
            # Убеждаемся что plan_data это список словарей
            if not isinstance(plan_data, list):
                raise ValueError("План должен быть списком шагов")
                
            for step in plan_data:
                if not isinstance(step, dict):
                    logger.warning(f"[ORCHESTRATOR] Пропущен шаг неверного формата: {step}")
                    continue
                nid = self.create_node(step.get('description', str(step)), step.get('agent'))
                if 'depends_on' in step:
                    for dep in step['depends_on']:
                        self.add_dependency(dep, nid)
                        
            logger.info("[ORCHESTRATOR] Граф задач построен")
        except Exception as e:
            # Fallback: один узел на всю задачу
            logger.warning(f"[ORCHESTRATOR] Ошибка парсинга плана: {e}, используем fallback")
            self.create_node(main_task, agent_list[0].name)

        # Шаг 2: Выполнение графа
        completed = set()
        iteration = 0
        
        while len(completed) < len(self.nodes):
            iteration += 1
            if iteration > 50:  # Защита от бесконечного цикла
                logger.error("[ORCHESTRATOR] Превышено максимальное количество итераций")
                break
                
            # Найти готовые к выполнению узлы (все зависимости выполнены)
            ready_nodes = []
            for node in self.nodes.values():
                if node.status == "pending":
                    if all(dep in completed for dep in node.dependencies):
                        ready_nodes.append(node)

            if not ready_nodes:
                logger.warning("[ORCHESTRATOR] Нет готовых узлов, выход из цикла")
                break

            logger.info(f"[ORCHESTRATOR] Запуск {len(ready_nodes)} узлов параллельно")
            
            # Параллельный запуск готовых узлов
            tasks = []
            for node in ready_nodes:
                node.status = "running"
                tasks.append(self._execute_node(node, main_task))

            results = await asyncio.gather(*tasks)

            for node, res in zip(ready_nodes, results):
                node.result = res
                node.status = "done" if "Ошибка" not in res else "failed"
                completed.add(node.id)
                await memory.save_episode(f"Node-{node.id}", f"{node.description}: {res[:100]}", node.status=="done")
                logger.info(f"[ORCHESTRATOR] Узел {node.id} завершен со статусом: {node.status}")

        # Сбор результатов
        final_report = "=== Отчет по графу задач ===\n"
        success_count = sum(1 for n in self.nodes.values() if n.status == "done")
        fail_count = len(self.nodes) - success_count
        
        for node in self.nodes.values():
            final_report += f"[{node.status.upper()}] Шаг {node.id}: {node.description}\nРезультат: {node.result}\n\n"

        logger.info(f"[ORCHESTRATOR] Задача завершена: успешно={success_count}, провалено={fail_count}")

        # Шаг 3: Самообучение (анализ итогов)
        if all(n.status == "done" for n in self.nodes.values()):
            learn_prompt = f"Задача '{main_task}' выполнена успешно. Извлеки 1-2 ключевых урока для будущего (коротко)."
            # Можно отправить любому агенту для генерации урока
            lesson_text = await planner.think_and_act(learn_prompt, ctx, [])
            await memory.save_lesson(main_task.split()[0], lesson_text) # Сохраняем по первому слову задачи
            logger.info("[ORCHESTRATOR] Урок сохранен в память")
        else:
            failed_ids = [n.id for n in self.nodes if n.status=='failed']
            await memory.save_lesson("failure_analysis", f"Задача '{main_task}' провалилась на шагах: {failed_ids}")
            logger.warning(f"[ORCHESTRATOR] Сохранен анализ неудачи: шаги {failed_ids}")

        return final_report

    async def execute_graph_stream(self, main_task: str, planner_agent=None, image_data=None):
        """
        Асинхронный генератор для стриминга результатов выполнения графа.
        1. Планирование -> статус 'planning'
        2. Выполнение узлов -> статусы 'tool_use', 'memory_search', 'code_execution', etc.
        3. Формирование ответа -> статус 'response'
        """
        # Шаг 1: Планирование
        yield {'type': 'status', 'status': 'planning'}
        
        plan_prompt = f"""
        Ты главный архитектор Nexus. Твоя задача: разбить задачу '{main_task}' на пошаговый план (граф).
        Доступные агенты: {list(self.agents.keys())}.
        Инструменты системы: {list(TOOLS.keys())}.

        Верни ТОЛЬКО JSON список шагов. Каждый шаг:
        {{
            "id": 0,
            "description": "Что сделать",
            "agent": "Имя агента или 'system_tool'",
            "depends_on": [список id предыдущих шагов]
        }}
        Если нужны системные команды (установка SDK, компиляция), используй агента 'System' или инструмент напрямую.
        """

        agent_list = list(self.agents.values())
        if not agent_list:
            yield {'type': 'error', 'text': 'Нет активных агентов для планирования.'}
            return

        planner = planner_agent if planner_agent else agent_list[0]
        ctx = await memory.get_context(main_task.split())
        
        logger.info("[ORCHESTRATOR/STREAM] Запуск планировщика...")
        plan_result = await planner.think_and_act(plan_prompt, ctx, [], image_data)

        # Парсинг плана
        try:
            clean_json = re.search(r'\[.*\]', plan_result, re.DOTALL)
            if clean_json:
                plan_data = json.loads(clean_json.group())
            else:
                plan_data = json.loads(plan_result)

            logger.info(f"[ORCHESTRATOR/STREAM] План получен: {len(plan_data)} шагов")
            
            # Убеждаемся что plan_data это список словарей
            if not isinstance(plan_data, list):
                raise ValueError("План должен быть списком шагов")
            
            for step in plan_data:
                if not isinstance(step, dict):
                    logger.warning(f"[ORCHESTRATOR/STREAM] Пропущен шаг неверного формата: {step}")
                    continue
                nid = self.create_node(step.get('description', str(step)), step.get('agent'))
                if 'depends_on' in step:
                    for dep in step['depends_on']:
                        self.add_dependency(dep, nid)
                        
            logger.info("[ORCHESTRATOR/STREAM] Граф задач построен")
        except Exception as e:
            logger.warning(f"[ORCHESTRATOR/STREAM] Ошибка парсинга плана: {e}, используем fallback")
            self.create_node(main_task, agent_list[0].name)

        # Шаг 2: Выполнение графа со стримингом
        completed = set()
        iteration = 0
        
        while len(completed) < len(self.nodes):
            iteration += 1
            if iteration > 50:
                logger.error("[ORCHESTRATOR/STREAM] Превышено максимальное количество итераций")
                break
                
            ready_nodes = []
            for node in self.nodes.values():
                if node.status == "pending":
                    if all(dep in completed for dep in node.dependencies):
                        ready_nodes.append(node)

            if not ready_nodes:
                logger.warning("[ORCHESTRATOR/STREAM] Нет готовых узлов, выход из цикла")
                break

            logger.info(f"[ORCHESTRATOR/STREAM] Запуск {len(ready_nodes)} узлов параллельно")
            
            # Стримим статус выполнения узлов
            for node in ready_nodes:
                node.status = "running"
                yield {'type': 'status', 'status': 'tool_use', 'node_id': node.id, 'description': node.description}
            
            # Параллельный запуск с перехватом результатов
            tasks = []
            for node in ready_nodes:
                tasks.append(self._execute_node_stream(node, main_task))
            
            # Собираем результаты по мере готовности
            for coro in asyncio.as_completed(tasks):
                result = await coro
                node = result['node']
                res = result['result']
                
                node.result = res
                node.status = "done" if "Ошибка" not in res else "failed"
                completed.add(node.id)
                
                # Отправляем результат узла
                yield {'type': 'message', 'text': f"✅ Узел {node.id}: {node.description}\n{res[:200]}"}
                
                await memory.save_episode(f"Node-{node.id}", f"{node.description}: {res[:100]}", node.status=="done")
                logger.info(f"[ORCHESTRATOR/STREAM] Узел {node.id} завершен со статусом: {node.status}")

        # Сбор финального отчета
        yield {'type': 'status', 'status': 'response'}
        
        final_report = "=== Отчет по графу задач ===\n"
        success_count = sum(1 for n in self.nodes.values() if n.status == "done")
        fail_count = len(self.nodes) - success_count
        
        for node in self.nodes.values():
            final_report += f"[{node.status.upper()}] Шаг {node.id}: {node.description}\nРезультат: {node.result}\n\n"

        logger.info(f"[ORCHESTRATOR/STREAM] Задача завершена: успешно={success_count}, провалено={fail_count}")
        
        # Отправляем финальный отчет
        yield {'type': 'message', 'text': final_report}

        # Самообучение
        if all(n.status == "done" for n in self.nodes.values()):
            learn_prompt = f"Задача '{main_task}' выполнена успешно. Извлеки 1-2 ключевых урока для будущего (коротко)."
            lesson_text = await planner.think_and_act(learn_prompt, ctx, [])
            await memory.save_lesson(main_task.split()[0], lesson_text)
            logger.info("[ORCHESTRATOR/STREAM] Урок сохранен в память")
            yield {'type': 'message', 'text': f"💡 Урок: {lesson_text[:200]}"}
        else:
            failed_ids = [n.id for n in self.nodes if n.status=='failed']
            await memory.save_lesson("failure_analysis", f"Задача '{main_task}' провалилась на шагах: {failed_ids}")
            logger.warning(f"[ORCHESTRATOR/STREAM] Сохранен анализ неудачи: шаги {failed_ids}")

    async def _execute_node_stream(self, node: TaskNode, global_task: str):
        """Обертка над _execute_node для возврата результата с информацией о узле"""
        result = await self._execute_node(node, global_task)
        return {'node': node, 'result': result}

    async def _execute_node(self, node: TaskNode, global_task: str):
        if node.assigned_agent and node.assigned_agent in self.agents:
            agent = self.agents[node.assigned_agent]
            ctx = await memory.get_context(global_task.split())
            # Передаем результаты зависимостей в контекст
            deps_context = ""
            for dep_id in node.dependencies:
                dep_node = self.nodes[dep_id]
                deps_context += f"Результат шага {dep_id} ({dep_node.description}): {dep_node.result}\n"

            full_prompt = f"{deps_context}\nВыполни шаг: {node.description}"
            return await agent.think_and_act(full_prompt, ctx, [])
        else:
            # Если агент не назначен или не найден, пытаемся выполнить как системную команду через Tool
            return f"Шаг требует назначения агента или инструмента."

# --- МОДЕЛЬ МИРА (ВНУТРЕННЕЕ СОСТОЯНИЕ СРЕДЫ) ---
class WorldModel:
    def __init__(self):
        self.state = {
            "installed_packages": [],
            "current_directory": os.getcwd(),
            "available_tools": list(TOOLS.keys()),
            "environment_vars": {"PATH": os.environ.get("PATH", "")},
            "last_error": None
        }
        self.history = []

    def get_state(self):
        return self.state

    def update_state(self, action, result):
        """Обновляет модель мира на основе действий"""
        self.history.append({"action": action, "result": result})
        if "установлен" in result.lower() or "installed" in result.lower():
            pass
        if "Ошибка" in result or "Error" in result:
            self.state["last_error"] = result

        if action.startswith("cd "):
             self.state["current_directory"] = os.getcwd()

    def get_recent_history(self, limit=5):
        return self.history[-limit:]

global_world_model = WorldModel()

# --- TREE OF THOUGHTS (OPTIMIZED FOR LOW-END) ---
class TreeOfThoughts:
    @staticmethod
    async def generate_thoughts(agent, task, context, n_branches=3):
        """Генерирует N альтернативных путей решения"""
        prompt = f"""
        Ты — стратегическое ядро Nexus. Задача: '{task}'.
        Контекст: {context}
        
        Сгенерируй ровно {n_branches} различных подхода к решению этой задачи.
        Каждый подход должен отличаться стратегией (напр. один через скрипты, другой через CLI, третий через проверку файлов).
        
        Верни ТОЛЬКО JSON массив:
        [
            {{
                "id": 1,
                "strategy_name": "Название подхода",
                "steps": ["шаг 1", "шаг 2"],
                "pros": ["преимущество 1"],
                "cons": ["риск 1"]
            }},
            ...
        ]
        """
        try:
            messages = [{"role": "system", "content": prompt}]
            response = await agent._call_llm(messages)
            clean_json = re.search(r'\[.*\]', response, re.DOTALL)
            if clean_json:
                thoughts = json.loads(clean_json.group())
                # Валидация и нормализация структуры
                validated = []
                for t in thoughts:
                    if not isinstance(t, dict):
                        continue
                    validated.append({
                        "id": t.get('id', len(validated)),
                        "strategy_name": str(t.get('strategy_name', 'Стратегия')),
                        "steps": [str(s) for s in t.get('steps', [])],
                        "pros": [str(p) for p in t.get('pros', [])],
                        "cons": [str(c) for c in t.get('cons', [])]
                    })
                return validated if validated else None
        except Exception as e:
            logging.warning(f"ToT генерация не удалась: {e}")
        
        # Fallback если LLM ошиблась
        return [
            {
                "id": 1, 
                "strategy_name": "Стандартный подход", 
                "steps": ["Анализ", "Выполнение", "Проверка"], 
                "pros": ["Надежно"], 
                "cons": ["Медленно"]
            }
        ]

    @staticmethod
    async def evaluate_thoughts(agent, task, thoughts):
        """Оценивает каждый путь и выбирает лучший"""
        best_choice = None
        max_score = -1
        
        for thought in thoughts:
            # Преобразуем элементы в строки перед join
            pros_str = ', '.join(str(p) for p in thought.get('pros', []))
            cons_str = ', '.join(str(c) for c in thought.get('cons', []))
            
            eval_prompt = f"""
            Оцени стратегию "{thought.get('strategy_name', 'Неизвестно')}" для задачи '{task}'.
            Плюсы: {pros_str}
            Минусы: {cons_str}
            
            Оцени по шкале 0-10:
            1. Вероятность успеха (Success Probability)
            2. Эффективность ресурсов (Resource Efficiency)
            3. Безопасность (Safety)
            
            Верни ТОЛЬКО JSON:
            {{
                "scores": {{ "success": 0, "efficiency": 0, "safety": 0 }},
                "total_score": 0,
                "reasoning": "Почему такая оценка"
            }}
            """
            try:
                messages = [{"role": "system", "content": eval_prompt}]
                response = await agent._call_llm(messages)
                clean_json = re.search(r'\{.*\}', response, re.DOTALL)
                if clean_json:
                    eval_data = json.loads(clean_json.group())
                    score = eval_data.get('total_score', 0)
                    
                    if score > max_score:
                        max_score = score
                        best_choice = {
                            "thought": thought,
                            "evaluation": eval_data
                        }
            except Exception as e:
                logging.warning(f"Ошибка оценки ветки ToT: {e}")
                continue
        
        if not best_choice:
            # Если оценка не удалась, берем первую ветку
            return {"thought": thoughts[0], "evaluation": {"reasoning": "Выбрано по умолчанию"}}
            
        return best_choice

# --- АГЕНТ С РЕФЛЕКСИЕЙ И МУЛЬТИМОДАЛЬНОСТЬЮ ---
class Agent:
    def __init__(self, name, role, model, base_url, api_key):
        self.name = name
        self.role = role
        self.model = model
        
        # Авто-коррекция для NVIDIA Build при создании агента
        is_nvidia = "nvidia.com" in base_url or (api_key and api_key.startswith("nvapi-"))
        if is_nvidia:
            # Принудительно устанавливаем правильный endpoint для NVIDIA
            self.base_url = "https://integrate.api.nvidia.com/v1".rstrip('/')
            # Добавляем префикс nvapi- если его нет
            if api_key and not api_key.startswith("nvapi-"):
                self.api_key = f"nvapi-{api_key}"
            else:
                self.api_key = api_key
            logger.info(f"[AGENT] Настроен агент NVIDIA: {name} -> {self.base_url}")
        else:
            self.base_url = base_url.rstrip('/')
            self.api_key = api_key
        
        self.world_model = global_world_model

    @staticmethod
    async def check_model_availability(model: str, base_url: str, api_key: str = "") -> Dict[str, Any]:
        """
        Проверяет доступность модели на указанном сервере и её способность корректно отвечать.
        Модель должна уметь возвращать валидный JSON по инструкции - это обязательное требование для instruct-моделей.
        """
        headers = {"Content-Type": "application/json"}
        
        # Специальная обработка для NVIDIA Build (NGC)
        is_nvidia = "nvidia.com" in base_url or (api_key and api_key.startswith("nvapi-"))
        if is_nvidia and api_key and not api_key.startswith("nvapi-"):
            api_key = f"nvapi-{api_key}"
        
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        
        # Для NVIDIA Build используем их специфичный endpoint
        if is_nvidia:
            base_url = "https://integrate.api.nvidia.com/v1"
        
        # Тестовый запрос: просим модель вернуть строго определённый JSON
        # Это проверяет, что модель понимает инструкции и может форматировать ответ
        # Instruct-модели должны точно следовать инструкциям
        test_prompt = '''Вы должны вернуть ТОЛЬКО следующий JSON объект, без какого-либо дополнительного текста, объяснений или markdown:
{"status": "ok", "model_test": true}'''
        
        test_messages = [
            {"role": "system", "content": "Вы помощник, который возвращает только валидный JSON. Никакого другого текста. Вы instruct-модель и должны точно следовать инструкциям."},
            {"role": "user", "content": test_prompt}
        ]
        
        payload = {
            "model": model,
            "messages": test_messages,
            "temperature": 0.0,
            "max_tokens": 50
        }
        
        # Выполняем тестовый запрос с требованием вернуть валидный JSON
        test_url = f"{base_url.rstrip('/')}/chat/completions"
        
        try:
            async with aiohttp.ClientSession() as session:
                # Увеличенный таймаут для NVIDIA API
                timeout = 45 if is_nvidia else 15
                async with session.post(test_url, json=payload, headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        content = data.get('choices', [{}])[0].get('message', {}).get('content', '')
                        
                        # Пытаемся распарсить ответ как JSON - это обязательное требование
                        try:
                            # Очищаем от markdown и лишнего текста
                            json_match = re.search(r'\{[^}]*\}', content, re.DOTALL)
                            if json_match:
                                result = json.loads(json_match.group())
                                if result.get('status') == 'ok' and result.get('model_test') is True:
                                    return {"available": True, "message": f"Модель '{model}' доступна и корректно выполняет инструкции (возвращает валидный JSON)"}
                            
                            # Если не удалось распарсить идеально - модель не подходит для работы с агентом
                            logger.warning(f"[MODEL_CHECK] Модель вернула некорректный формат: {content[:200]}")
                            return {"available": False, "message": f"Модель '{model}' не является instruct-моделью (не вернула ожидаемый JSON формат). Используйте instruct-версию модели."}
                        except (json.JSONDecodeError, AttributeError) as e:
                            logger.warning(f"[MODEL_CHECK] Модель вернула невалидный JSON: {content[:200]}, ошибка: {e}")
                            return {"available": False, "message": f"Модель '{model}' не является instruct-моделью (не смогла вернуть валидный JSON). Выберите другую instruct-модель."}
                    elif resp.status == 404:
                        return {"available": False, "message": f"Модель '{model}' не найдена на сервере"}
                    elif resp.status == 401:
                        return {"available": False, "message": "Неверный API ключ или отсутствует авторизация. Для NVIDIA используйте ключ с префиксом 'nvapi-'"}
                    elif resp.status == 403:
                        return {"available": False, "message": f"Доступ запрещён. Проверьте права доступа к модели '{model}'"}
                    elif resp.status == 503:
                        return {"available": False, "message": f"Модель '{model}' временно недоступна (сервер перегружен)"}
                    else:
                        error_text = await resp.text()
                        return {"available": False, "message": f"Ошибка API {resp.status}: {error_text[:200]}"}
        except asyncio.TimeoutError:
            return {"available": False, "message": "Таймаут подключения к серверу моделей"}
        except Exception as e:
            return {"available": False, "message": f"Ошибка подключения: {str(e)}"}

    async def think_and_simulate(self, task, context):
        """Этап рефлексии: Tree of Thoughts + виртуальная симуляция."""
        logger.info(f"[{self.name}] 🌳 Запуск Tree of Thoughts для задачи: {task[:50]}...")
        
        # 1. Генерация ветвей (ToT)
        thoughts = await TreeOfThoughts.generate_thoughts(self, task, context, n_branches=3)
        logger.info(f"[{self.name}] Сгенерировано {len(thoughts)} стратегий.")
        
        # 2. Оценка и выбор лучшей (ToT)
        best_choice = await TreeOfThoughts.evaluate_thoughts(self, task, thoughts)
        chosen_strategy = best_choice['thought']
        evaluation = best_choice['evaluation']
        
        logger.info(f"[{self.name}] ✅ Выбрана стратегия: {chosen_strategy['strategy_name']}")
        logger.info(f"[{self.name}] 💡 Обоснование: {evaluation.get('reasoning', 'Нет обоснования')}")

        # 3. Ментальная симуляция на основе ЛУЧШЕЙ стратегии
        # Преобразуем элементы в строки перед join для защиты от ошибок типа данных
        steps_str = ', '.join(str(s) for s in chosen_strategy.get('steps', []))
        pros_str = ', '.join(str(p) for p in chosen_strategy.get('pros', []))
        cons_str = ', '.join(str(c) for c in chosen_strategy.get('cons', []))
        
        system_prompt = f"""Ты — рефлексирующее ядро агента '{self.name}' ({self.role}).
        Ты выбрал стратегию: {chosen_strategy.get('strategy_name', 'Неизвестно')}.
        План: {steps_str}
        Преимущества: {pros_str}
        Риски: {cons_str}
        
        Текущее состояние мира:
        {json.dumps(self.world_model.get_state(), indent=2, ensure_ascii=False)}

        Задача: {task}
        
        Проведи финальную проверку выбранного плана:
        1. Есть ли скрытые зависимости?
        2. Хватает ли прав доступа?
        3. Нужна ли установка пакетов?

        Верни ТОЛЬКО JSON:
        {{
            "feasible": boolean,
            "missing_requirements": ["список недостающего"],
            "pre_action_plan": ["шаги по исправлению"],
            "risk_assessment": "high/medium/low",
            "optimized_strategy": "детализированный план на основе выбранной ветки"
        }}
        """

        messages = [{"role": "system", "content": system_prompt}]
        response = await self._call_llm(messages)

        try:
            clean_json = response.replace('```json', '').replace('```', '').strip()
            simulation_result = json.loads(clean_json)
            # Добавляем информацию о выбранной стратегии в результат
            simulation_result['chosen_tot_strategy'] = chosen_strategy
            logger.info(f"[{self.name}] Рефлексия завершена: выполнимо={simulation_result.get('feasible')}, риск={simulation_result.get('risk_assessment')}")
            return simulation_result
        except Exception as e:
            logger.warning(f"[{self.name}] Ошибка парсинга симуляции: {e}")
            return {
                "feasible": True,
                "missing_requirements": [],
                "pre_action_plan": [],
                "risk_assessment": "unknown",
                "optimized_strategy": chosen_strategy['steps'],
                "chosen_tot_strategy": chosen_strategy
            }

    async def _call_llm(self, messages, image_data=None):
        """Универсальный вызов LLM с поддержкой мультимодальности"""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload_messages = list(messages)

        # Если есть изображение, добавляем его в последний пользовательский запрос
        if image_data and payload_messages and payload_messages[-1]["role"] == "user":
            last_content = payload_messages[-1]["content"]
            if isinstance(last_content, str):
                payload_messages[-1]["content"] = [
                    {"type": "text", "text": last_content},
                    {"type": "image_url", "image_url": {"url": image_data}}
                ]

        payload = {
            "model": self.model,
            "messages": payload_messages,
            "temperature": 0.7,
            "stream": False
        }

        url = f"{self.base_url}/chat/completions"

        # Увеличенный таймаут для NVIDIA API
        is_nvidia = "nvidia.com" in self.base_url or (self.api_key and self.api_key.startswith("nvapi-"))
        timeout = 120 if is_nvidia else 60

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"API Error {resp.status}: {text}")
                data = await resp.json()
                return data['choices'][0]['message']['content']

    async def execute_with_reflection(self, task, context, history, image_data=None):
        """Основной цикл с обязательной рефлексией и самоисцелением"""
        logger.info(f"[{self.name}] 🧠 Начало фазы рефлексии...")

        # 1. Рефлексия
        reflection = await self.think_and_simulate(task, context)
        
        # Защита от случая когда reflection это список вместо словаря
        if isinstance(reflection, list):
            logger.warning(f"[{self.name}] Рефлексия вернула список вместо словаря, используем fallback")
            reflection = {
                "feasible": True,
                "missing_requirements": [],
                "pre_action_plan": [],
                "risk_assessment": "unknown",
                "optimized_strategy": str(reflection)
            }
        
        logger.info(f"[{self.name}] ✅ Выполнимо: {reflection.get('feasible')}")
        logger.info(f"[{self.name}] ⚠️ Риски: {reflection.get('risk_assessment')}")

        # 2. Предварительное исправление (Self-Healing)
        missing = reflection.get('missing_requirements')
        if missing and isinstance(missing, list):
            logger.info(f"[{self.name}] ❌ Нехватает: {reflection['missing_requirements']}")
            if reflection.get('pre_action_plan'):
                logger.info(f"[{self.name}] 🔧 Запуск плана восстановления...")
                for step in reflection['pre_action_plan']:
                    # Попытка автоматически установить пакеты
                    if "install" in step.lower() or "package" in step.lower():
                        # Извлекаем имя пакета (упрощенно)
                        pkg_match = re.search(r'[\w-]+', step)
                        if pkg_match:
                            pkg = pkg_match.group(0)
                            res = await TOOLS['install_pkg']['func']("pip", pkg)
                            logger.info(f"[{self.name}] Установка {pkg}: {res[:50]}")

        # 3. Основной цикл ReAct
        system_prompt = f"""Ты агент '{self.name}'. Роль: {self.role}.
        Стратегия (из рефлексии): {reflection.get('optimized_strategy')}
        Состояние мира: {json.dumps(self.world_model.get_state(), ensure_ascii=False)}

        Доступные инструменты: {list(TOOLS.keys())}.
        Контекст: {context}
        История: {history}

        ЗАДАЧА: {task}

        ИНСТРУКЦИЯ:
        1. Думай шаг за шагом.
        2. Инструменты: {{"action": "tool_name", "args": {{...}}}}
        3. Ответ: {{"action": "final_answer", "content": "..."}}
        4. ТОЛЬКО JSON.
        """

        current_history = list(history)
        # Добавляем задачу с учетом изображения
        user_msg = {"role": "user", "content": task}
        current_history.append(user_msg)

        max_iterations = 10
        for i in range(max_iterations):
            try:
                logger.debug(f"[{self.name}] Итерация ReAct {i+1}/{max_iterations}")
                
                # Передаем image_data только в первом запросе после задачи
                img_to_pass = image_data if i == 0 and image_data else None

                # Формируем сообщения для API
                api_messages = [{"role": "system", "content": system_prompt}] + current_history

                content = await self._call_llm(api_messages, image_data=img_to_pass)

                # Парсинг ответа
                try:
                    clean_content = content.replace('```json', '').replace('```', '').strip()
                    response_json = json.loads(clean_content)

                    if response_json.get('action') == 'final_answer':
                        logger.info(f"[{self.name}] Задача завершена успешно")
                        self.world_model.update_state("final_answer", response_json['content'])
                        return response_json['content']

                    elif response_json.get('action') in TOOLS:
                        tool_name = response_json['action']
                        args = response_json.get('args', {})
                        
                        # Валидация аргументов для инструментов
                        if not isinstance(args, dict):
                            logger.warning(f"[{self.name}] Аргументы инструмента должны быть словарем")
                            args = {}

                        logger.info(f"[{self.name}] Вызов инструмента: {tool_name}")
                        
                        # Маппинг имен аргументов для совместимости
                        # run_shell_command ожидает 'cmd', но агент может передать 'command'
                        if tool_name == 'run_shell' and 'command' in args and 'cmd' not in args:
                            args['cmd'] = args.pop('command')
                        
                        try:
                            res = await TOOLS[tool_name]['func'](**args)
                        except TypeError as te:
                            # Если переданы неверные аргументы, пытаемся исправить
                            logger.warning(f"[{self.name}] Ошибка аргументов инструмента: {te}")
                            res = f"Ошибка вызова инструмента: неверные аргументы. Ожидались: {list(TOOLS[tool_name]['func'].__code__.co_varnames[:TOOLS[tool_name]['func'].__code__.co_argcount])}"

                        # Обновляем модель мира
                        self.world_model.update_state(tool_name, res)

                        current_history.append({"role": "assistant", "content": content})
                        current_history.append({"role": "user", "content": f"Результат {tool_name}: {res}"})
                        continue
                    else:
                        logger.warning(f"[{self.name}] Неизвестное действие: {response_json}")
                        return f"Неизвестное действие: {response_json}"

                except json.JSONDecodeError as je:
                    logger.error(f"[{self.name}] Ошибка парсинга JSON: {je}")
                    # Попытка вытащить JSON из текста
                    match = re.search(r'\{.*\}', content, re.DOTALL)
                    if match:
                        try:
                            response_json = json.loads(match.group())
                            if response_json.get('action') == 'final_answer':
                                logger.info(f"[{self.name}] Задача завершена (извлечено из текста)")
                                return response_json['content']
                        except: pass
                    return f"Модель вернула не JSON: {content}"

            except Exception as e:
                logger.error(f"[{self.name}] Ошибка выполнения: {str(e)}")
                return f"Ошибка выполнения: {str(e)}"

        logger.warning(f"[{self.name}] Превышено количество итераций ({max_iterations})")
        return "Превышено количество итераций."

    # Для обратной совместимости со старым кодом оркестратора
    async def think_and_act(self, task, context, history, image_data=None):
        return await self.execute_with_reflection(task, context, history, image_data)

# --- QUART ROUTES (ASYNC) ---

agents_registry = {}
orchestrator = GraphOrchestrator(agents_registry)

@app.route('/')
async def index():
    with open('index.html', 'r', encoding='utf-8') as f:
        return f.read()

@app.route('/api/agents', methods=['POST'])
async def add_agent():
    data = await request.json
    name = data.get('name')
    role = data.get('role')
    model = data.get('model')
    base_url = data.get('base_url')
    api_key = data.get('api_key', '')

    if not all([name, role, model, base_url]):
        return jsonify({"error": "Заполните все поля: имя, роль, модель и URL сервера"}), 400

    # Обязательная проверка доступности модели
    logger.info(f"[AGENT_ADD] Проверка доступности модели '{model}' на сервере '{base_url}'...")
    availability = await Agent.check_model_availability(model, base_url, api_key)
    
    if not availability.get('available'):
        logger.warning(f"[AGENT_ADD] Модель недоступна: {availability.get('message')}")
        return jsonify({
            "error": "Model not available",
            "details": availability.get('message'),
            "suggestion": "Проверьте: 1) Название модели 2) URL сервера 3) API ключ 4) Сетевое подключение"
        }), 400
    
    logger.info(f"[AGENT_ADD] ✅ Модель доступна: {availability.get('message')}")

    agent = Agent(name, role, model, base_url, api_key)
    agents_registry[name] = agent
    # Обновляем оркестратор
    orchestrator.agents = agents_registry
    return jsonify({
        "status": "success", 
        "message": f"Агент '{name}' успешно добавлен",
        "model_check": availability.get('message')
    })

@app.route('/api/agents', methods=['GET'])
async def list_agents():
    return jsonify(list(agents_registry.keys()))

@app.route('/api/run', methods=['POST'])
async def run():
    data = await request.json
    task = data.get('task')
    image_data = data.get('image_data')  # Получаем изображение

    if not task:
        return jsonify({"error": "Task missing"}), 400

    if not agents_registry:
        return jsonify({"error": "No agents available. Add an agent first."}), 400

    # Прямой асинхронный запуск без ThreadPoolExecutor
    try:
        results = await orchestrator.execute_graph(task, None, image_data)
        if isinstance(results, str) or isinstance(results, list):
            return jsonify({"results": results})
        elif isinstance(results, dict):
            return jsonify(results)
        else:
            return jsonify({"results": str(results)})
    except Exception as e:
        logger.error(f"[API/RUN] Ошибка выполнения: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/memory', methods=['GET'])
async def get_memory():
    return jsonify({"short_term": memory.short_term})

@app.route('/api/chat/stream', methods=['POST'])
async def chat_stream():
    """Стриминг ответов агента через Server-Sent Events"""
    from quart import Response
    
    data = await request.json
    task = data.get('task')
    image_data = data.get('image_data')
    
    if not task:
        return jsonify({"error": "Task missing"}), 400
    
    if not agents_registry:
        return jsonify({"error": "No agents available"}), 400
    
    async def generate():
        try:
            # Отправляем статус начала
            yield f"data: {json.dumps({'type': 'status', 'content': '🔄 Инициализация...'})}\n\n"
            
            # Получаем оркестратор и выполняем граф
            status_messages = {
                'planning': '🧠 Планирование...',
                'tool_use': '🛠️ Использование инструментов...',
                'memory_search': '🔍 Поиск в памяти...',
                'code_execution': '💻 Выполнение кода...',
                'analysis': '📊 Анализ...',
                'response': '✍️ Формирование ответа...'
            }
            
            # Запускаем выполнение графа с перехватом статусов
            async for event in orchestrator.execute_graph_stream(task, None, image_data):
                if isinstance(event, dict):
                    if event.get('type') == 'status':
                        status_key = event.get('status', 'processing')
                        msg = status_messages.get(status_key, '⏳ Обработка...')
                        yield f"data: {json.dumps({'type': 'status', 'content': msg})}\n\n"
                    elif event.get('type') == 'message':
                        yield f"data: {json.dumps({'type': 'message', 'content': event.get('text', '')})}\n\n"
                else:
                    # Обычный текст - считаем частью сообщения
                    yield f"data: {json.dumps({'type': 'message', 'content': str(event)})}\n\n"
            
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            
        except Exception as e:
            logger.error(f"[STREAM] Ошибка: {str(e)}")
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream')

@app.before_serving
async def init_db():
    """Инициализация БД перед запуском сервера"""
    await memory.graph._init_async()
    # Создаем таблицы для episodes и lessons если их нет
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS episodes
                     (timestamp REAL, role TEXT, content TEXT, success BOOLEAN)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS lessons
                     (keyword TEXT PRIMARY KEY, lesson_text TEXT, count INTEGER DEFAULT 1)''')
        await db.commit()
    logger.info("[INIT] База данных полностью инициализирована.")

if __name__ == '__main__':
    logger.info("Starting Nexus Multi-Agent System with Graph Orchestrator (ASYNC)...")
    logger.info("Features: Auto-learning, Dynamic Graph Planning, Self-Healing, Tool Use.")
    logger.info("Logging enabled: all actions will be saved to nexus_agent.log")
    logger.info("Access web interface at http://127.0.0.1:5000")

    app.run(debug=True, port=5000)

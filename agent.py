import os
import sys
import json
import time
import sqlite3
import hashlib
import threading
import asyncio
import subprocess
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

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
    from flask import Flask, request, jsonify, render_template_string
except ImportError:
    logger.info("Установка Flask...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "flask"])
    from flask import Flask, request, jsonify, render_template_string

# --- КОНФИГУРАЦИЯ ---
DB_PATH = "nexus_memory.db"
app = Flask(__name__)

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
        self._load_graph()
        logger.info("[GRAPH] Графовая память инициализирована.")

    def _init_db(self):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS graph_nodes
                     (id TEXT PRIMARY KEY, label TEXT, type TEXT, properties TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS graph_edges
                     (source_id TEXT, relation TEXT, target_id TEXT,
                      PRIMARY KEY (source_id, relation, target_id))''')
        conn.commit()
        conn.close()

    def _get_node_id(self, label: str) -> str:
        return hashlib.md5(label.lower().strip().encode()).hexdigest()[:12]

    def add_node(self, label: str, node_type: str = "concept") -> GraphNode:
        node_id = self._get_node_id(label)
        if node_id not in self.nodes:
            node = GraphNode(node_id, label, node_type)
            self.nodes[node_id] = node
            self._save_node(node)
            logger.debug(f"[GRAPH] Добавлен узел: {label} ({node_type})")
        else:
            self.nodes[node_id].properties["access_count"] += 1
        return self.nodes[node_id]

    def add_edge(self, source_label: str, target_label: str, relation: str = "related_to"):
        src = self.add_node(source_label)
        tgt = self.add_node(target_label)
        src.add_edge(relation, tgt.id)
        # Обратная связь для неиерархических отношений
        if relation not in ["causes", "requires", "uses"]:
            tgt.add_edge(f"reverse_{relation}", src.id)
        self._save_edge(src.id, relation, tgt.id)
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
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO graph_nodes (id, label, type, properties) VALUES (?, ?, ?, ?)",
                  (node.id, node.label, node.type, json.dumps(node.properties)))
        conn.commit()
        conn.close()

    def _save_edge(self, src: str, rel: str, tgt: str):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO graph_edges (source_id, relation, target_id) VALUES (?, ?, ?)",
                  (src, rel, tgt))
        conn.commit()
        conn.close()

    def _load_graph(self):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, label, type, properties FROM graph_nodes")
        for row in c.fetchall():
            node = GraphNode(row[0], row[1], row[2])
            node.properties = json.loads(row[3])
            self.nodes[row[0]] = node
        c.execute("SELECT source_id, relation, target_id FROM graph_edges")
        for row in c.fetchall():
            if row[0] in self.nodes:
                self.nodes[row[0]].add_edge(row[1], row[2])
        conn.close()
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

    def save_episode(self, role, content, success=True):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        ts = time.time()
        c.execute("INSERT INTO episodes (timestamp, role, content, success) VALUES (?, ?, ?, ?)",
                  (ts, role, content, success))
        conn.commit()
        conn.close()
        self.add_short_term(role, content)
        
        # Обновление графа
        self.graph.add_node(role, "agent_role")
        if "ошибка" in content.lower() or "error" in content.lower() or "fail" in content.lower():
            self.graph.add_node("Error", "system_state")
            self.graph.add_edge(role, "Error", "encountered")
        logger.info(f"[MEMORY] Эпизод сохранен: {role} | Успех: {success}")

    def save_lesson(self, keyword, lesson_text):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT count FROM lessons WHERE keyword=?", (keyword,))
        row = c.fetchone()
        if row:
            c.execute("UPDATE lessons SET count=count+1 WHERE keyword=?", (keyword,))
            logger.info(f"[MEMORY] Обновлен урок: {keyword}")
        else:
            c.execute("INSERT INTO lessons (keyword, lesson_text) VALUES (?, ?)", (keyword, lesson_text))
            logger.info(f"[MEMORY] Создан урок: {keyword}")
        conn.commit()
        conn.close()
        
        # Сохранение в граф
        self.graph.add_edge(keyword, lesson_text[:50], "has_lesson")

    def get_lessons(self, keywords):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        lessons = []
        for kw in keywords:
            c.execute("SELECT lesson_text FROM lessons WHERE keyword LIKE ?", (f"%{kw}%",))
            rows = c.fetchall()
            lessons.extend([r[0] for r in rows])
        conn.close()
        return lessons

    def get_context(self, task_keywords=None):
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

        lessons = self.get_lessons(task_keywords) if task_keywords else []
        if lessons:
            context += "\n=== ИЗВЛЕЧЕННЫЕ УРОКИ ===\n"
            for l in lessons:
                context += f"- {l}\n"

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT role, content, success FROM episodes ORDER BY timestamp DESC LIMIT 5")
        rows = c.fetchall()
        conn.close()

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
            stdout, stderr = await proc.communicate(timeout=120)
            output = stdout.decode() + stderr.decode()
            logger.info(f"[TOOL] Команда выполнена, вывод: {output[:200]}...")
            return output
        except asyncio.TimeoutError:
            logger.error("[TOOL] Таймаут выполнения команды")
            return "Таймаут выполнения команды."
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

    async def execute_graph(self, main_task: str, planner_agent, image_data=None):
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

        planner = agent_list[0] # Временное решение: первый агент как планировщик

        ctx = memory.get_context(main_task.split())
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
            for step in plan_data:
                nid = self.create_node(step['description'], step.get('agent'))
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
                memory.save_episode(f"Node-{node.id}", f"{node.description}: {res[:100]}", node.status=="done")
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
            memory.save_lesson(main_task.split()[0], lesson_text) # Сохраняем по первому слову задачи
            logger.info("[ORCHESTRATOR] Урок сохранен в память")
        else:
            failed_ids = [n.id for n in self.nodes if n.status=='failed']
            memory.save_lesson("failure_analysis", f"Задача '{main_task}' провалилась на шагах: {failed_ids}")
            logger.warning(f"[ORCHESTRATOR] Сохранен анализ неудачи: шаги {failed_ids}")

        return final_report

    async def _execute_node(self, node: TaskNode, global_task: str):
        if node.assigned_agent and node.assigned_agent in self.agents:
            agent = self.agents[node.assigned_agent]
            ctx = memory.get_context(global_task.split())
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
                return json.loads(clean_json.group())
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
            eval_prompt = f"""
            Оцени стратегию "{thought['strategy_name']}" для задачи '{task}'.
            Плюсы: {', '.join(thought['pros'])}
            Минусы: {', '.join(thought['cons'])}
            
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
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.world_model = global_world_model

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
        system_prompt = f"""Ты — рефлексирующее ядро агента '{self.name}' ({self.role}).
        Ты выбрал стратегию: {chosen_strategy['strategy_name']}.
        План: {', '.join(chosen_strategy['steps'])}
        Преимущества: {', '.join(chosen_strategy['pros'])}
        Риски: {', '.join(chosen_strategy['cons'])}
        
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

        url = f"{self.base_url}/v1/chat/completions"

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=120) as resp:
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
        logger.info(f"[{self.name}] ✅ Выполнимо: {reflection.get('feasible')}")
        logger.info(f"[{self.name}] ⚠️ Риски: {reflection.get('risk_assessment')}")

        # 2. Предварительное исправление (Self-Healing)
        if reflection.get('missing_requirements'):
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

                        logger.info(f"[{self.name}] Вызов инструмента: {tool_name}")
                        res = await TOOLS[tool_name]['func'](**args)

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

# --- FLASK ROUTES ---

agents_registry = {}
orchestrator = GraphOrchestrator(agents_registry)

@app.route('/')
def index():
    with open('index.html', 'r', encoding='utf-8') as f:
        return f.read()

@app.route('/api/agents', methods=['POST'])
def add_agent():
    data = request.json
    name = data.get('name')
    role = data.get('role')
    model = data.get('model')
    base_url = data.get('base_url')
    api_key = data.get('api_key', '')

    if not all([name, role, model, base_url]):
        return jsonify({"error": "Missing fields"}), 400

    agent = Agent(name, role, model, base_url, api_key)
    agents_registry[name] = agent
    # Обновляем оркестратор
    orchestrator.agents = agents_registry
    return jsonify({"status": "success", "message": f"Agent {name} added"})

@app.route('/api/agents', methods=['GET'])
def list_agents():
    return jsonify(list(agents_registry.keys()))

@app.route('/api/run', methods=['POST'])
def run():
    data = request.json
    task = data.get('task')
    image_data = data.get('image_data')  # Получаем изображение

    if not task:
        return jsonify({"error": "Task missing"}), 400

    if not agents_registry:
        return jsonify({"error": "No agents available. Add an agent first."}), 400

    def run_async_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # Передаем изображение в оркестратор
        res = loop.run_until_complete(orchestrator.execute_graph(task, None, image_data))
        loop.close()
        return res

    # Используем ThreadPoolExecutor для неблокирующего запуска
    future = app.config.get('executor', ThreadPoolExecutor(max_workers=5)).submit(run_async_loop)
    try:
        results = future.result(timeout=300) # 5 минут на сложные задачи
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/memory', methods=['GET'])
def get_memory():
    return jsonify({"short_term": memory.short_term})

if __name__ == '__main__':
    logger.info("Starting Nexus Multi-Agent System with Graph Orchestrator...")
    logger.info("Features: Auto-learning, Dynamic Graph Planning, Self-Healing, Tool Use.")
    logger.info("Logging enabled: all actions will be saved to nexus_agent.log")
    logger.info("Access web interface at http://127.0.0.1:5000")

    # Сохраняем executor в конфиге app
    app.config['executor'] = ThreadPoolExecutor(max_workers=5)

    app.run(debug=True, port=5000, threaded=True)

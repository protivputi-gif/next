import os, sys, json, time, hashlib, asyncio, subprocess, re, logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("nexus.log", encoding='utf-8'), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("Nexus")

# Auto-install dependencies
for pkg, name in [('aiohttp', 'aiohttp'), ('aiosqlite', 'aiosqlite'), ('quart', 'quart')]:
    try: exec(f"import {name}")
    except ImportError:
        logger.info(f"Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

import aiohttp, aiosqlite
from quart import Quart, request, jsonify, Response

DB_PATH = "nexus_memory.db"
app = Quart(__name__)
agents_registry = {}

# Optimized config - FAST by default
CONFIG = {"max_iterations": 6, "cache_enabled": True, "simple_threshold": 150}
response_cache = {}

class GraphMemory:
    """Lightweight graph memory (RAM + SQLite)"""
    def __init__(self):
        self.nodes = {}
    
    async def _init_async(self):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('CREATE TABLE IF NOT EXISTS graph_nodes (id TEXT PRIMARY KEY, label TEXT, type TEXT, properties TEXT)')
            await db.execute('CREATE TABLE IF NOT EXISTS graph_edges (source TEXT, relation TEXT, target TEXT, PRIMARY KEY(source, relation, target))')
            await db.execute('CREATE TABLE IF NOT EXISTS episodes (timestamp REAL, role TEXT, content TEXT, success BOOLEAN)')
            await db.execute('CREATE TABLE IF NOT EXISTS lessons (keyword TEXT PRIMARY KEY, lesson_text TEXT, count INTEGER DEFAULT 1)')
            await db.commit()
            # Load nodes from DB
            async with db.execute('SELECT * FROM graph_nodes') as cursor:
                for row in await cursor.fetchall():
                    node = GraphNode(row[0], row[1], row[2])
                    node.properties = json.loads(row[3]) if row[3] else {}
                    self.nodes[row[0]] = node
            logger.info("[GRAPH] Loaded from DB")
    
    async def add_node(self, label, node_type="concept"):
        node_id = hashlib.md5(label.encode()).hexdigest()[:12]
        if node_id not in self.nodes:
            self.nodes[node_id] = GraphNode(node_id, label, node_type)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute('INSERT OR REPLACE INTO graph_nodes VALUES (?,?,?,?)', 
                    (node_id, label, node_type, json.dumps(self.nodes[node_id].properties)))
                await db.commit()
        return node_id
    
    async def add_edge(self, source_label, target_label, relation="related"):
        src_id = await self.add_node(source_label)
        tgt_id = await self.add_node(target_label)
        self.nodes[src_id].add_edge(relation, tgt_id)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('INSERT OR IGNORE INTO graph_edges VALUES (?,?,?)', (src_id, relation, tgt_id))
            await db.commit()
    
    def get_neighbors(self, label, depth=1):
        node_id = hashlib.md5(label.encode()).hexdigest()[:12]
        if node_id not in self.nodes: return []
        return [self.nodes.get(tid) for rid in self.nodes[node_id].edges.values() for tid in rid][:5]

class GraphNode:
    def __init__(self, node_id, label, node_type="concept"):
        self.id, self.label, self.type = node_id, label, node_type
        self.properties = {"created_at": time.time(), "access_count": 0}
        self.edges = defaultdict(list)
    
    def add_edge(self, relation, target_id):
        if target_id not in self.edges[relation]: self.edges[relation].append(target_id)

memory = GraphMemory()

async def save_episode(role, content, success=True):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO episodes VALUES (?,?,?,?)', (time.time(), role, content, success))
        await db.commit()
    logger.info(f"[MEMORY] Episode saved: {role}")

async def get_context(keywords=None):
    ctx = "=== Recent History ===\n"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT role, content, success FROM episodes ORDER BY timestamp DESC LIMIT 5') as cursor:
            for r in await cursor.fetchall():
                ctx += f"[{'OK' if r[2] else 'FAIL'}] {r[0]}: {r[1][:60]}...\n"
    if keywords and memory.nodes:
        ctx += "\n=== Knowledge Graph ===\n"
        for kw in keywords:
            for n in memory.get_neighbors(kw):
                ctx += f"- {n.label} ({n.type})\n"
    return ctx

async def save_lesson(keyword, text):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO lessons VALUES (?, ?, COALESCE((SELECT count FROM lessons WHERE keyword=?),0)+1)', 
            (keyword, text, keyword))
        await db.commit()
    logger.info(f"[MEMORY] Lesson saved: {keyword}")

# Tools
class Tools:
    ARG_MAP = {"filename":"path","file":"path","filepath":"path","script":"code","text":"content","command":"cmd","pkg_name":"package"}
    
    @staticmethod
    async def run_python(code: str):
        logger.info(f"[TOOL] Python exec ({len(code)} chars)")
        try:
            scope = {"__builtins__": __builtins__}
            exec(code, scope, scope)
            return str(scope.get('result', 'Executed'))
        except Exception as e: return f"Error: {e}"
    
    @staticmethod
    async def run_shell(cmd: str):
        logger.info(f"[TOOL] Shell: {cmd}")
        if any(d in cmd for d in ["rm -rf /", "mkfs", "dd if="]): return "Blocked: dangerous command"
        try:
            proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            return (stdout + stderr).decode()[:2000]
        except asyncio.TimeoutError: return "Timeout (120s)"
        except Exception as e: return f"Error: {e}"
    
    @staticmethod
    async def install_pkg(manager: str, package: str):
        logger.info(f"[TOOL] Install {package} via {manager}")
        cmd = {"apt": f"apt-get update && apt-get install -y {package}", 
               "pip": f"pip install {package}", 
               "sdk": f"sdkmanager \"{package}\""}.get(manager)
        return await Tools.run_shell(cmd) if cmd else f"Unknown manager: {manager}"
    
    @staticmethod
    async def read_file(path: str):
        if not os.path.exists(path): return "File not found"
        try:
            with open(path, 'r', encoding='utf-8') as f: return f.read()[:5000]
        except Exception as e: return f"Error: {e}"
    
    @staticmethod
    async def write_file(path: str, content: str):
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f: f.write(content)
            return f"Written: {path}"
        except Exception as e: return f"Error: {e}"
    
    @staticmethod
    async def fetch_url(url: str):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp: return (await resp.text())[:5000]
        except Exception as e: return f"Error: {e}"
    
    @classmethod
    def normalize(cls, tool, args):
        if not isinstance(args, dict): return {}
        return {cls.ARG_MAP.get(k, k): v for k, v in args.items()}

TOOLS = {
    "run_python": {"desc": "Execute Python code", "func": Tools.run_python},
    "run_shell": {"desc": "Run shell command", "func": Tools.run_shell},
    "install_pkg": {"desc": "Install package (apt/pip/sdk)", "func": Tools.install_pkg},
    "read_file": {"desc": "Read file", "func": Tools.read_file},
    "write_file": {"desc": "Write file", "func": Tools.write_file},
    "fetch_url": {"desc": "Fetch URL", "func": Tools.fetch_url},
}

class Agent:
    def __init__(self, name, role, model, base_url, api_key):
        self.name, self.role, self.model = name, role, model
        is_nvidia = "nvidia.com" in base_url or (api_key and api_key.startswith("nvapi-"))
        # Use NVIDIA default only if explicitly requested or if api_key starts with nvapi-
        if is_nvidia:
            self.base_url = "https://integrate.api.nvidia.com/v1"
            # Only add nvapi- prefix if it's not already there
            if api_key and not api_key.startswith("nvapi-"):
                self.api_key = f"nvapi-{api_key}"
            else:
                self.api_key = api_key
        else:
            # Keep provided base_url or use empty string
            self.base_url = base_url.rstrip('/') if base_url else ""
            self.api_key = api_key
        logger.info(f"[AGENT] Created: {name} @ {self.base_url}")
    
    async def _call_llm(self, messages, image_data=None):
        headers = {"Content-Type": "application/json"}
        if self.api_key: headers["Authorization"] = f"Bearer {self.api_key}"
        
        payload_msgs = list(messages)
        if image_data and payload_msgs and payload_msgs[-1]["role"] == "user":
            last = payload_msgs[-1]["content"]
            if isinstance(last, str):
                payload_msgs[-1]["content"] = [{"type": "text", "text": last}, {"type": "image_url", "image_url": {"url": image_data}}]
        
        payload = {"model": self.model, "messages": payload_msgs, "temperature": 0.7}
        url = f"{self.base_url}/chat/completions"
        timeout = 120 if "nvidia" in self.base_url else 60
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
                if resp.status != 200: raise Exception(f"API {resp.status}: {await resp.text()}")
                data = await resp.json()
                return data['choices'][0]['message']['content']
    
    def _parse_json(self, text):
        """Robust JSON extraction"""
        clean = text.replace('```json', '').replace('```', '').strip()
        start = clean.find('{')
        if start == -1: return None
        depth, end = 0, start
        for i, c in enumerate(clean[start:], start):
            depth += 1 if c == '{' else -1 if c == '}' else 0
            if depth == 0: end = i + 1; break
        try: return json.loads(clean[start:end])
        except: return None
    
    async def execute(self, task, context, history=None, image_data=None):
        """Unified ReAct loop - simple and fast (non-streaming)"""
        cache_key = hashlib.md5(f"{task}:{context[:100]}".encode()).hexdigest()
        if CONFIG["cache_enabled"] and cache_key in response_cache:
            logger.info(f"[{self.name}] ⚡ Cache hit")
            return response_cache[cache_key]
        
        is_simple = len(task) < CONFIG["simple_threshold"]
        logger.info(f"[{self.name}] 🚀 Executing (simple={is_simple})")
        
        sys_prompt = f"""You are '{self.name}' ({self.role}). Use tools to complete tasks.
Tools: {list(TOOLS.keys())}
Context: {context}

TASK: {task}

RULES:
1. Output ONLY valid JSON. No text outside {{}}. No markdown.
2. Tool format: {{"action":"tool_name","args":{{"param":"value"}}}}
3. Final answer: {{"action":"final_answer","content":"result"}}
4. Be concise and direct.

Examples:
{{"action":"run_python","args":{{"code":"print(2+2)"}}}}
{{"action":"final_answer","content":"Done"}}

Output JSON now:"""
        
        hist = list(history or [])
        hist.append({"role": "user", "content": task})
        
        for i in range(CONFIG["max_iterations"]):
            try:
                msgs = [{"role": "system", "content": sys_prompt}] + hist
                content = await self._call_llm(msgs, image_data if i == 0 else None)
                
                parsed = self._parse_json(content)
                if not parsed:
                    logger.warning(f"[{self.name}] Invalid JSON: {content[:100]}")
                    hist.append({"role": "assistant", "content": content})
                    hist.append({"role": "user", "content": "Invalid JSON. Try again."})
                    continue
                
                action = parsed.get('action')
                if action == 'final_answer':
                    result = parsed.get('content', 'No content')
                    logger.info(f"[{self.name}] ✅ Completed")
                    if CONFIG["cache_enabled"]:
                        response_cache[cache_key] = result
                        if len(response_cache) > 100: response_cache.pop(next(iter(response_cache)))
                    return result
                
                if action in TOOLS:
                    args = Tools.normalize(action, parsed.get('args', {}))
                    logger.info(f"[{self.name}] 🛠️ {action}({list(args.keys())})")
                    try: result = await TOOLS[action]['func'](**args)
                    except TypeError as e: result = f"Arg error: {e}"
                    hist.append({"role": "assistant", "content": content})
                    hist.append({"role": "user", "content": f"{action} result: {result}"})
                    continue
                
                logger.warning(f"[{self.name}] Unknown action: {action}")
                hist.append({"role": "user", "content": f"Unknown: {action}"})
                
            except Exception as e:
                logger.error(f"[{self.name}] Error: {e}")
                return f"Error: {e}"
        
        return "Max iterations exceeded"
    
    async def execute_stream(self, task, context, history=None, image_data=None):
        """Streaming version of execute"""
        cache_key = hashlib.md5(f"{task}:{context[:100]}".encode()).hexdigest()
        if CONFIG["cache_enabled"] and cache_key in response_cache:
            logger.info(f"[{self.name}] ⚡ Cache hit (stream)")
            yield {'type': 'message', 'content': response_cache[cache_key]}
            return
        
        is_simple = len(task) < CONFIG["simple_threshold"]
        logger.info(f"[{self.name}] 🚀 Executing stream (simple={is_simple})")
        
        sys_prompt = f"""You are '{self.name}' ({self.role}). Use tools.
Tools: {list(TOOLS.keys())}
Context: {context}
TASK: {task}
Output JSON only: {{"action":"tool","args":{{}}}} or {{"action":"final_answer","content":"result"}}"""
        
        hist = list(history or [])
        hist.append({"role": "user", "content": task})
        
        for i in range(CONFIG["max_iterations"]):
            try:
                msgs = [{"role": "system", "content": sys_prompt}] + hist
                content = await self._call_llm(msgs, image_data if i == 0 else None)
                parsed = self._parse_json(content)
                
                if not parsed:
                    hist.append({"role": "assistant", "content": content})
                    hist.append({"role": "user", "content": "Invalid JSON."})
                    continue
                
                action = parsed.get('action')
                if action == 'final_answer':
                    result = parsed.get('content', '')
                    if CONFIG["cache_enabled"]:
                        response_cache[cache_key] = result
                        if len(response_cache) > 100: response_cache.pop(next(iter(response_cache)))
                    yield {'type': 'message', 'content': result}
                    return
                
                if action in TOOLS:
                    args = Tools.normalize(action, parsed.get('args', {}))
                    yield {'type': 'action', 'content': f"Using {action}..."}
                    try: result = await TOOLS[action]['func'](**args)
                    except TypeError as e: result = str(e)
                    yield {'type': 'action', 'content': f"Result: {str(result)[:200]}"}
                    hist.append({"role": "assistant", "content": content})
                    hist.append({"role": "user", "content": f"{action}: {result}"})
                    continue
                
            except Exception as e:
                yield {'type': 'error', 'content': str(e)}
                return
        
        yield {'type': 'message', 'content': "Max iterations"}

# Simple orchestrator
async def execute_graph(task, agent, image_data=None):
    """Execute task directly (no complex planning)"""
    ctx = await get_context(task.split())
    result = await agent.execute(task, ctx, image_data=image_data)
    await save_episode("agent", str(result)[:200], "Error" not in str(result))
    return result

# Web routes
@app.route('/')
async def index():
    with open('index.html', 'r') as f: return f.read()

@app.route('/api/agents', methods=['POST'])
async def add_agent():
    data = await request.get_json()
    name = data.get('name', 'Agent1')
    if name in agents_registry: return jsonify({'error': 'Exists'}), 400
    
    agent = Agent(name, data.get('role', 'Assistant'), data.get('model', 'meta/llama-3.1-8b-instruct'),
                  data.get('base_url', 'https://integrate.api.nvidia.com/v1'), data.get('api_key', ''))
    
    # Test availability - skip check if api_key is empty or 'skip' for testing
    api_key = data.get('api_key', '')
    if api_key and api_key != 'skip':
        ok = await check_model(agent.model, agent.base_url, agent.api_key)
        if not ok['available']: 
            logger.warning(f"[API] Model check failed: {ok['message']}")
            # Still allow adding for testing purposes
            # return jsonify({'error': ok['message']}), 400
    
    agents_registry[name] = agent
    logger.info(f"[API] Agent added: {name}")
    return jsonify({'success': True, 'name': name})

@app.route('/api/agents', methods=['GET'])
async def list_agents():
    return jsonify({'agents': list(agents_registry.keys())})

@app.route('/api/chat', methods=['POST'])
async def chat():
    data = await request.get_json()
    task = data.get('task', '')
    agent_name = data.get('agent')
    image = data.get('image')
    
    if not task: return jsonify({'error': 'No task'}), 400
    if agent_name and agent_name not in agents_registry: return jsonify({'error': 'Agent not found'}), 404
    
    agent = agents_registry.get(agent_name)
    if not agent: return jsonify({'error': 'No agents available. Add one first.'}), 400
    
    result = await execute_graph(task, agent, image)
    return jsonify({'response': result})

@app.route('/api/chat/stream', methods=['POST'])
async def chat_stream():
    from quart import Response
    data = await request.get_json()
    task = data.get('task', '')
    agent_name = data.get('agent')
    image = data.get('image')
    
    if not task: return jsonify({'error': 'No task'}), 400
    agent = agents_registry.get(agent_name)
    if not agent: return jsonify({'error': 'No agents'}), 400
    
    async def gen():
        try:
            ctx = await get_context(task.split())
            if hasattr(agent, 'execute_stream'):
                async for event in agent.execute_stream(task, ctx, image_data=image):
                    yield f"data: {json.dumps(event)}\n\n"
            else:
                result = await agent.execute(task, ctx, image_data=image)
                yield f"data: {json.dumps({'response': result})}\n\n"
            yield "data: {\"done\": true}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return Response(gen(), mimetype='text/event-stream')

@app.route('/api/memory', methods=['GET'])
async def get_memory():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT keyword, lesson_text, count FROM lessons ORDER BY count DESC LIMIT 10') as cur:
            lessons = [{'keyword': r[0], 'text': r[1], 'count': r[2]} for r in await cur.fetchall()]
    return jsonify({'lessons': lessons, 'graph_nodes': len(memory.nodes)})

async def check_model(model, base_url, api_key=""):
    """Check if model is available"""
    is_nvidia = "nvidia.com" in base_url or (api_key and api_key.startswith("nvapi-"))
    if is_nvidia:
        base_url = "https://integrate.api.nvidia.com/v1"
        if not api_key: return {'available': False, 'message': 'NVIDIA API key required'}
        # Only add nvapi- prefix if it's not already there
        if not api_key.startswith("nvapi-"):
            api_key = f"nvapi-{api_key}"
    
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"} if api_key else {"Content-Type": "application/json"}
    test_msg = [{"role": "system", "content": "Return ONLY: {\"ok\":true}"}, {"role": "user", "content": "Test"}]
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{base_url.rstrip('/')}/chat/completions", 
                json={"model": model, "messages": test_msg, "max_tokens": 10}, 
                headers=headers, timeout=30) as resp:
                if resp.status == 200: return {'available': True, 'message': 'OK'}
                return {'available': False, 'message': f"Error {resp.status}"}
    except Exception as e: return {'available': False, 'message': str(e)}

@app.before_serving
async def init_db():
    await memory._init_async()
    logger.info("[INIT] Database ready")

if __name__ == '__main__':
    logger.info("🚀 Nexus Agent v2.0 - Fast & Simple")
    logger.info("Open http://127.0.0.1:5000")
    app.run(debug=True, port=5000)

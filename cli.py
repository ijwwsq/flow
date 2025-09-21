# ============================================================================
# logger.py
# ============================================================================

import logging
import sys
from pathlib import Path


class FlowLogger:
    def __init__(self, log_file="flow.log"):
        self.log_file = Path(log_file)
        self.setup_logging()
    
    def setup_logging(self):
        file_handler = logging.FileHandler(self.log_file)
        file_formatter = logging.Formatter('%(asctime)s %(message)s')
        file_handler.setFormatter(file_formatter)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        
        self.logger = logging.getLogger('flow')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def info(self, msg):
        self.logger.info(msg)
    
    def error(self, msg):
        self.logger.error(msg)
    
    def warning(self, msg):
        self.logger.warning(msg)

# ============================================================================
# parser.py
# ============================================================================

import yaml
from pathlib import Path
from typing import Dict, List, Any
from dataclasses import dataclass


@dataclass
class Task:
    id: str
    run: str
    depends_on: List[str]
    
    def __post_init__(self):
        if self.depends_on is None:
            self.depends_on = []


class PipelineParser:
    def __init__(self, logger):
        self.logger = logger
    
    def parse(self, pipeline_file: str) -> List[Task]:
        pipeline_path = Path(pipeline_file)
        
        if not pipeline_path.exists():
            raise FileNotFoundError(f"file not found: {pipeline_file}")
        
        try:
            with open(pipeline_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if 'tasks' not in data:
                raise ValueError("no tasks found")
            
            tasks = []
            task_ids = set()
            
            for task_data in data['tasks']:
                if 'id' not in task_data or 'run' not in task_data:
                    raise ValueError("task missing id or run")
                
                task_id = task_data['id']
                if task_id in task_ids:
                    raise ValueError(f"duplicate task: {task_id}")
                
                task_ids.add(task_id)
                
                task = Task(
                    id=task_id,
                    run=task_data['run'],
                    depends_on=task_data.get('depends_on', [])
                )
                tasks.append(task)
            
            for task in tasks:
                for dep in task.depends_on:
                    if dep not in task_ids:
                        raise ValueError(f"task {task.id} depends on missing {dep}")
            
            self.logger.info(f"loaded {len(tasks)} tasks")
            return tasks
            
        except yaml.YAMLError as e:
            raise ValueError(f"bad yaml: {e}")

# ============================================================================
# scheduler.py
# ============================================================================

from typing import List, Set, Dict
from collections import defaultdict, deque
from parser import Task


class TaskScheduler:
    def __init__(self, logger):
        self.logger = logger
    
    def build_dependency_graph(self, tasks: List[Task]) -> Dict[str, List[str]]:
        graph = defaultdict(list)
        
        for task in tasks:
            for dep in task.depends_on:
                graph[dep].append(task.id)
        
        return dict(graph)
    
    def detect_cycles(self, tasks: List[Task]) -> bool:
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        
        for task in tasks:
            in_degree[task.id] = 0
        
        for task in tasks:
            for dep in task.depends_on:
                graph[dep].append(task.id)
                in_degree[task.id] += 1
        
        queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
        processed = 0
        
        while queue:
            current = queue.popleft()
            processed += 1
            
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        has_cycle = processed != len(tasks)
        
        if has_cycle:
            self.logger.error("cycle detected")
        
        return has_cycle
    
    def get_execution_order(self, tasks: List[Task]) -> List[List[str]]:
        if self.detect_cycles(tasks):
            raise ValueError("cyclic dependencies")
        
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        
        for task in tasks:
            in_degree[task.id] = 0
        
        for task in tasks:
            for dep in task.depends_on:
                graph[dep].append(task.id)
                in_degree[task.id] += 1
        
        levels = []
        remaining_tasks = set(task.id for task in tasks)
        
        while remaining_tasks:
            current_level = [task_id for task_id in remaining_tasks 
                           if in_degree[task_id] == 0]
            
            if not current_level:
                raise ValueError("unresolvable dependencies")
            
            levels.append(current_level)
            
            for task_id in current_level:
                remaining_tasks.remove(task_id)
                for neighbor in graph[task_id]:
                    in_degree[neighbor] -= 1
        
        return levels

# ============================================================================
# executor.py
# ============================================================================

import subprocess
import concurrent.futures
import time
from enum import Enum
from typing import Dict, List
from pathlib import Path
import json
from parser import Task


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"  
    SUCCESS = "done"
    FAILED = "failed"
    RETRYING = "retrying"


class TaskResult:
    def __init__(self, task_id: str):
        self.task_id = task_id
        self.status = TaskStatus.PENDING
        self.start_time = None
        self.end_time = None
        self.attempts = 0
        self.output = ""
        self.error = ""


class TaskExecutor:
    def __init__(self, logger, max_workers=4, retries=0):
        self.logger = logger
        self.max_workers = max_workers
        self.retries = retries
        self.results: Dict[str, TaskResult] = {}
        self.state_file = Path("flow_state.json")
    
    def save_state(self):
        state = {}
        for task_id, result in self.results.items():
            state[task_id] = {
                'status': result.status.value,
                'attempts': result.attempts,
                'start_time': result.start_time,
                'end_time': result.end_time
            }
        
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    def load_state(self) -> bool:
        if not self.state_file.exists():
            return False
        
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            
            for task_id, task_state in state.items():
                result = TaskResult(task_id)
                result.status = TaskStatus(task_state['status'])
                result.attempts = task_state['attempts']
                result.start_time = task_state['start_time']
                result.end_time = task_state['end_time']
                self.results[task_id] = result
            
            self.logger.info(f"resumed {len(state)} tasks")
            return True
            
        except Exception as e:
            self.logger.warning(f"resume failed: {e}")
            return False
    
    def execute_task(self, task: Task) -> TaskResult:
        if task.id not in self.results:
            self.results[task.id] = TaskResult(task.id)
        
        result = self.results[task.id]
        
        if result.status == TaskStatus.SUCCESS:
            return result
        
        max_attempts = self.retries + 1
        
        while result.attempts < max_attempts:
            result.attempts += 1
            result.status = TaskStatus.RETRYING if result.attempts > 1 else TaskStatus.RUNNING
            result.start_time = time.time()
            
            self.logger.info(f"{task.id}: {result.status.value}")
            
            try:
                process = subprocess.Popen(
                    task.run,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                output_lines = []
                for line in process.stdout:
                    line = line.rstrip()
                    if line:
                        print(f"  {task.id} | {line}")
                        output_lines.append(line)
                
                process.wait(timeout=3600)
                result.output = '\n'.join(output_lines)
                result.error = ""
                
                if process.returncode == 0:
                    result.status = TaskStatus.SUCCESS
                    self.logger.info(f"{task.id}: done")
                    self.save_state()
                    return result
                else:
                    raise subprocess.CalledProcessError(process.returncode, task.run, 
                                                      result.output, "")
                    
            except subprocess.TimeoutExpired:
                process.kill()
                result.error = "timeout"
                self.logger.error(f"{task.id}: timeout")
                
            except subprocess.CalledProcessError as e:
                result.error = f"exit {e.returncode}"
                self.logger.error(f"{task.id}: failed")
                
            except Exception as e:
                result.error = str(e)
                self.logger.error(f"{task.id}: error")
        
        result.status = TaskStatus.FAILED
        result.end_time = time.time()
        self.save_state()
        return result
    
    def execute_level(self, tasks: List[Task]) -> List[TaskResult]:
        tasks_to_run = []
        for task in tasks:
            if task.id not in self.results or self.results[task.id].status != TaskStatus.SUCCESS:
                tasks_to_run.append(task)
        
        if not tasks_to_run:
            return [self.results[task.id] for task in tasks]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(tasks_to_run))) as executor:
            future_to_task = {executor.submit(self.execute_task, task): task for task in tasks_to_run}
            results = []
            
            for future in concurrent.futures.as_completed(future_to_task):
                result = future.result()
                results.append(result)
        
        for task in tasks:
            if task not in tasks_to_run:
                results.append(self.results[task.id])
        
        return results
    
    def get_status_summary(self) -> Dict:
        summary = {status.value: 0 for status in TaskStatus}
        
        for result in self.results.values():
            summary[result.status.value] += 1
        
        return summary

# ============================================================================
# cli.py
# ============================================================================

import click
import sys
from pathlib import Path
from logger import FlowLogger
from parser import PipelineParser
from scheduler import TaskScheduler
from executor import TaskExecutor, TaskStatus


@click.group()
@click.version_option()
def cli():
    """flow - simple task orchestrator"""
    pass


@cli.command()
@click.argument('pipeline_file', type=click.Path(exists=True))
@click.option('--max-workers', '-w', default=4, help='parallel tasks')
@click.option('--retries', '-r', default=0, help='retry attempts')
@click.option('--resume', is_flag=True, help='resume from failure')
def run(pipeline_file, max_workers, retries, resume):
    """run pipeline"""
    logger = FlowLogger()
    logger.info("flow starting")
    logger.info(f"running {pipeline_file}")
    
    try:
        parser = PipelineParser(logger)
        tasks = parser.parse(pipeline_file)
        
        scheduler = TaskScheduler(logger)
        execution_levels = scheduler.get_execution_order(tasks)
        
        executor = TaskExecutor(logger, max_workers, retries)
        
        if resume:
            executor.load_state()
        
        task_map = {task.id: task for task in tasks}
        
        total_failed = 0
        for level_task_ids in execution_levels:
            # Check dependencies before running level
            blocked_tasks = []
            for task_id in level_task_ids:
                task = task_map[task_id]
                for dep in task.depends_on:
                    dep_result = executor.results.get(dep)
                    if not dep_result or dep_result.status != TaskStatus.SUCCESS:
                        logger.error(f"{task_id}: blocked by {dep}")
                        blocked_tasks.append(task_id)
                        break
            
            # Remove blocked tasks from this level
            runnable_tasks = [task_id for task_id in level_task_ids if task_id not in blocked_tasks]
            
            if not runnable_tasks:
                if blocked_tasks:
                    continue  # Skip this level, all tasks blocked
            
            # Execute runnable tasks
            if runnable_tasks:
                level_tasks = [task_map[task_id] for task_id in runnable_tasks]
                level_results = executor.execute_level(level_tasks)
                
                level_failed = sum(1 for r in level_results if r.status == TaskStatus.FAILED)
                total_failed += level_failed
        
        if total_failed > 0:
            logger.error("failed")
            sys.exit(1)
        else:
            logger.info("done")
            if executor.state_file.exists():
                executor.state_file.unlink()
            
    except Exception as e:
        logger.error(f"failed: {e}")
        sys.exit(1)


@cli.command()
def status():
    """show task status"""
    logger = FlowLogger()
    
    executor = TaskExecutor(logger)
    if not executor.load_state():
        logger.info("no previous run")
        return
    
    if not executor.results:
        logger.info("no tasks found")
        return
    
    for task_id, result in executor.results.items():
        logger.info(f"{task_id}: {result.status.value}")


if __name__ == '__main__':
    cli()
import subprocess
import concurrent.futures
import time
from enum import Enum
from typing import Dict, List
from pathlib import Path
from parser import Task
import json


class TaskStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"  
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


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
        self.state_file = Path("taskflow_state.json")
    
    def save_state(self):
        """Сохраняет состояние выполнения для возможности resume"""
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
        """Загружает сохраненное состояние"""
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
            
            self.logger.info(f"Loaded previous state: {len(state)} tasks")
            return True
            
        except Exception as e:
            self.logger.warning(f"Failed to load state: {e}")
            return False
    
    def execute_task(self, task: Task) -> TaskResult:
        """Выполняет одну задачу"""
        if task.id not in self.results:
            self.results[task.id] = TaskResult(task.id)
        
        result = self.results[task.id]
        
        # Если задача уже выполнена успешно, пропускаем
        if result.status == TaskStatus.SUCCESS:
            self.logger.info(f"Task {task.id}: Already completed ✓")
            return result
        
        max_attempts = self.retries + 1
        
        while result.attempts < max_attempts:
            result.attempts += 1
            result.status = TaskStatus.RETRYING if result.attempts > 1 else TaskStatus.RUNNING
            result.start_time = time.time()
            
            status_msg = f"Task {task.id}: {result.status.value}"
            if result.attempts > 1:
                status_msg += f" (attempt {result.attempts}/{max_attempts})"
            self.logger.info(status_msg)
            
            try:
                # Выполняем команду с выводом в реальном времени
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
                # Читаем вывод построчно и показываем в реальном времени
                for line in process.stdout:
                    line = line.rstrip()
                    if line:
                        print(f"  [{task.id}] {line}")  # Выводим с префиксом задачи
                        output_lines.append(line)
                
                process.wait(timeout=3600)
                result.output = '\n'.join(output_lines)
                result.error = ""
                
                if process.returncode == 0:
                    result.status = TaskStatus.SUCCESS
                    self.logger.info(f"Task {task.id}: SUCCESS ✓")
                    self.save_state()
                    return result
                else:
                    raise subprocess.CalledProcessError(process.returncode, task.run, 
                                                    result.output, "")
                
                result.end_time = time.time()
                result.output = process.stdout
                result.error = process.stderr
                
                if process.returncode == 0:
                    result.status = TaskStatus.SUCCESS
                    self.logger.info(f"Task {task.id}: SUCCESS ✓")
                    self.save_state()
                    return result
                else:
                    raise subprocess.CalledProcessError(process.returncode, task.run, 
                                                      process.stdout, process.stderr)
                    
            except subprocess.TimeoutExpired:
                result.error = "Task timed out after 1 hour"
                self.logger.error(f"Task {task.id}: TIMEOUT")
                
            except subprocess.CalledProcessError as e:
                result.error = f"Exit code {e.returncode}: {e.stderr}"
                self.logger.error(f"Task {task.id}: FAILED - {result.error}")
                
            except Exception as e:
                result.error = str(e)
                self.logger.error(f"Task {task.id}: ERROR - {result.error}")
        
        # Все попытки исчерпаны
        result.status = TaskStatus.FAILED
        result.end_time = time.time()
        self.logger.error(f"Task {task.id}: FAILED after {result.attempts} attempts ✗")
        self.save_state()
        return result
    
    def execute_level(self, tasks: List[Task]) -> List[TaskResult]:
        """Выполняет задачи одного уровня параллельно"""
        # Фильтруем только те задачи, которые нужно выполнить
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
        
        # Добавляем результаты уже выполненных задач
        for task in tasks:
            if task not in tasks_to_run:
                results.append(self.results[task.id])
        
        return results
    
    def get_status_summary(self) -> Dict:
        """Возвращает сводку по статусам задач"""
        summary = {status.value: 0 for status in TaskStatus}
        
        for result in self.results.values():
            summary[result.status.value] += 1
        
        return summary
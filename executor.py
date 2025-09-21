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
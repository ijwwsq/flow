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
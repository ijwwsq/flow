from typing import List, Set, Dict
from collections import defaultdict, deque
from parser import Task


class TaskScheduler:
    def __init__(self, logger):
        self.logger = logger
    
    def build_dependency_graph(self, tasks: List[Task]) -> Dict[str, List[str]]:
        """Строит граф зависимостей"""
        graph = defaultdict(list)
        
        for task in tasks:
            for dep in task.depends_on:
                graph[dep].append(task.id)
        
        return dict(graph)
    
    def detect_cycles(self, tasks: List[Task]) -> bool:
        """Проверяет наличие циклов в графе зависимостей"""
        # Создаем граф зависимостей (обратный)
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        
        # Инициализируем все задачи
        for task in tasks:
            in_degree[task.id] = 0
        
        # Строим граф
        for task in tasks:
            for dep in task.depends_on:
                graph[dep].append(task.id)
                in_degree[task.id] += 1
        
        # Топологическая сортировка для обнаружения циклов
        queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
        processed = 0
        
        while queue:
            current = queue.popleft()
            processed += 1
            
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # Если обработали не все задачи, значит есть цикл
        has_cycle = processed != len(tasks)
        
        if has_cycle:
            self.logger.error("Detected cycle in task dependencies!")
        
        return has_cycle
    
    def get_execution_order(self, tasks: List[Task]) -> List[List[str]]:
        """Возвращает порядок выполнения задач по уровням (для параллельного выполнения)"""
        if self.detect_cycles(tasks):
            raise ValueError("Cannot execute pipeline with cyclic dependencies")
        
        # Создаем граф и счетчик входящих рёбер
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        
        # Инициализируем все задачи
        for task in tasks:
            in_degree[task.id] = 0
        
        # Строим граф
        for task in tasks:
            for dep in task.depends_on:
                graph[dep].append(task.id)
                in_degree[task.id] += 1
        
        # Группируем задачи по уровням выполнения
        levels = []
        remaining_tasks = set(task.id for task in tasks)
        
        while remaining_tasks:
            # Находим задачи без зависимостей на текущем уровне
            current_level = [task_id for task_id in remaining_tasks 
                           if in_degree[task_id] == 0]
            
            if not current_level:
                raise ValueError("Unable to resolve dependencies")
            
            levels.append(current_level)
            
            # Удаляем текущие задачи и обновляем счетчики
            for task_id in current_level:
                remaining_tasks.remove(task_id)
                for neighbor in graph[task_id]:
                    in_degree[neighbor] -= 1
        
        self.logger.info(f"Execution plan: {len(levels)} levels")
        for i, level in enumerate(levels):
            self.logger.info(f"  Level {i + 1}: {', '.join(level)}")
        
        return levels
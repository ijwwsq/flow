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
        """Парсит YAML файл с пайплайном и возвращает список задач"""
        pipeline_path = Path(pipeline_file)
        
        if not pipeline_path.exists():
            raise FileNotFoundError(f"Pipeline file not found: {pipeline_file}")
        
        try:
            with open(pipeline_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if 'tasks' not in data:
                raise ValueError("Pipeline must contain 'tasks' section")
            
            tasks = []
            task_ids = set()
            
            for task_data in data['tasks']:
                if 'id' not in task_data or 'run' not in task_data:
                    raise ValueError("Each task must have 'id' and 'run' fields")
                
                task_id = task_data['id']
                if task_id in task_ids:
                    raise ValueError(f"Duplicate task ID: {task_id}")
                
                task_ids.add(task_id)
                
                task = Task(
                    id=task_id,
                    run=task_data['run'],
                    depends_on=task_data.get('depends_on', [])
                )
                tasks.append(task)
            
            # Проверяем, что все зависимости существуют
            for task in tasks:
                for dep in task.depends_on:
                    if dep not in task_ids:
                        raise ValueError(f"Task '{task.id}' depends on non-existent task '{dep}'")
            
            self.logger.info(f"Parsed {len(tasks)} tasks from {pipeline_file}")
            return tasks
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format: {e}")
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
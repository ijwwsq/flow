import click
import sys
from pathlib import Path
from logger import TaskFlowLogger
from parser import PipelineParser
from scheduler import TaskScheduler
from executor import TaskExecutor, TaskStatus


@click.group()
@click.version_option()
def cli():
    """TaskFlow — мини-оркестратор задач"""
    pass


@cli.command()
@click.argument('pipeline_file', type=click.Path(exists=True))
@click.option('--max-workers', '-w', default=4, help='Количество параллельных задач')
@click.option('--retries', '-r', default=0, help='Количество попыток при падении')
@click.option('--resume', is_flag=True, help='Продолжить выполнение с места падения')
def run(pipeline_file, max_workers, retries, resume):
    """Запускает выполнение пайплайна"""
    logger = TaskFlowLogger()
    logger.info("🚀 TaskFlow starting...")
    logger.info(f"Pipeline: {pipeline_file}")
    logger.info(f"Max workers: {max_workers}, Retries: {retries}, Resume: {resume}")
    
    try:
        # Парсинг пайплайна
        parser = PipelineParser(logger)
        tasks = parser.parse(pipeline_file)
        
        # Создание плана выполнения
        scheduler = TaskScheduler(logger)
        execution_levels = scheduler.get_execution_order(tasks)
        
        # Подготовка исполнителя
        executor = TaskExecutor(logger, max_workers, retries)
        
        # Загрузка состояния если нужно
        if resume:
            executor.load_state()
        
        # Создаем словарь для быстрого поиска задач
        task_map = {task.id: task for task in tasks}
        
        # Выполнение по уровням
        total_failed = 0
        for level_idx, level_task_ids in enumerate(execution_levels):
            logger.info(f"\n--- Level {level_idx + 1} ---")
            
            # Проверяем зависимости
            can_execute_level = True
            for task_id in level_task_ids:
                task = task_map[task_id]
                for dep in task.depends_on:
                    dep_result = executor.results.get(dep)
                    if not dep_result or dep_result.status != TaskStatus.SUCCESS:
                        logger.error(f"Cannot execute {task_id}: dependency {dep} not successful")
                        can_execute_level = False
            
            if not can_execute_level:
                logger.error("Stopping execution due to failed dependencies")
                break
            
            # Выполняем уровень
            level_tasks = [task_map[task_id] for task_id in level_task_ids]
            level_results = executor.execute_level(level_tasks)
            
            # Проверяем результаты уровня
            level_failed = sum(1 for r in level_results if r.status == TaskStatus.FAILED)
            total_failed += level_failed
            
            if level_failed > 0:
                logger.error(f"Level {level_idx + 1}: {level_failed} tasks failed")
                if level_failed == len(level_results):
                    logger.error("All tasks in level failed. Stopping execution.")
                    break
        
        # Финальная статистика
        summary = executor.get_status_summary()
        logger.info("\n=== Execution Summary ===")
        for status, count in summary.items():
            if count > 0:
                logger.info(f"{status}: {count}")
        
        if total_failed > 0:
            logger.error(f"\n❌ Pipeline completed with {total_failed} failed tasks")
            sys.exit(1)
        else:
            logger.info("\n✅ Pipeline completed successfully!")
            # Удаляем файл состояния при успешном завершении
            if executor.state_file.exists():
                executor.state_file.unlink()
            
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        sys.exit(1)


@cli.command()
def status():
    """Показывает статус задач из последнего запуска"""
    logger = TaskFlowLogger()
    
    executor = TaskExecutor(logger)
    if not executor.load_state():
        logger.info("No previous execution state found")
        return
    
    if not executor.results:
        logger.info("No task results found")
        return
    
    logger.info("=== Task Status ===")
    for task_id, result in executor.results.items():
        status_icon = {
            'SUCCESS': '✅',
            'FAILED': '❌', 
            'RUNNING': '🔄',
            'PENDING': '⏳',
            'RETRYING': '🔄'
        }.get(result.status.value, '❓')
        
        info = f"{status_icon} {task_id}: {result.status.value}"
        if result.attempts > 0:
            info += f" (attempts: {result.attempts})"
        
        logger.info(info)
    
    summary = executor.get_status_summary()
    logger.info("\n=== Summary ===")
    for status, count in summary.items():
        if count > 0:
            logger.info(f"{status}: {count}")


if __name__ == '__main__':
    cli()
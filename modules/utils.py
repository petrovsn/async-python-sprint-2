#декоратор для отлова исключений и логирования вызовов
import functools
import time
import logging 


logger = logging.getLogger('scheduler app')
logging.basicConfig()
logger.setLevel(logging.ERROR)

def execution_controller(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            timestamp = time.perf_counter()
            result = func(*args, **kwargs)
            logger.info(f"{func.__name__} called successfully, execution time:{time.perf_counter()-timestamp}" ) 
            return result
        except Exception as e:
            logger.error(f"{func.__name__} caused exception: {repr(e)}" ) 
            return None
    return wrapper
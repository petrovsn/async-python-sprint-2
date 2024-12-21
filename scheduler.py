import os
import time
from datetime import datetime, timedelta
from multiprocessing import Manager
from queue import Queue
from threading import Lock, Thread
from typing import Generator

import requests

from job import Job, JobLoader, JobStatus
from modules.utils import execution_controller, logger

# class syntax

        

class Scheduler:
    @execution_controller
    def __init__(self, pool_size=10, directory_name = "scheduler_dir"):
        self.queue: Queue[Job] = Queue()
        self.active_pool = {}
        self.pool_size = pool_size
        self.completed = {}
        self.loop_thread_lock = Lock()
        
        self._is_running = False

        if not os.path.exists(directory_name):
            os.mkdir(directory_name)
        self.directory_name = directory_name

    @property
    def is_running(self):
        return self._is_running
    
    @is_running.setter
    def is_running(self, value:bool):
        self.loop_thread_lock.acquire()
        self._is_running =value
        self.loop_thread_lock.release()

    @execution_controller
    def schedule(self, task: Job) -> None:
        self.loop_thread_lock.acquire()
        self.queue.put(task)
        self.loop_thread_lock.release()

    @execution_controller
    def run(self):
        self.is_running = True
        self.loop_thread = Thread(target=self.execution_loop, daemon= True)
        self.loop_thread.start()

    """
    основной метод класса: крутится в цикле: 
    1. попытаться подгрузить задачи из очереди, 
    2. выполнить по шагу из активных задач
    3. проверить активные задачи на завершенность, выгрузить выполненные
    """
    @execution_controller
    def execution_loop(self) -> None:
        while self.is_running:
            self.loop_thread_lock.acquire()
            self.try_to_add_new_jobs_to_active()
            self.active_job_cycle()
            self.clear_active_pool()
            self.loop_thread_lock.release()
    
    @execution_controller
    def clear_active_pool(self):
        to_del = []
        for name in self.active_pool:
            #если задача выполнена или аварийно завершена, она удаляется из активного цикла
            if self.active_pool[name].status in [JobStatus.COMPLETED, JobStatus.BROKEN]:
                to_del.append(name)

            #если задача выполнена успешно, она сохраняется в словарь выполненных задач
            if self.active_pool[name].status ==JobStatus.COMPLETED:
                self.completed[name] = self.active_pool[name]
        for name in to_del:
            self.active_pool.pop(name)
        
    @execution_controller
    def can_add_job_to_active(self, job:Job):
        if len(self.active_pool) == self.pool_size: return False
        if job.start_at:
            if job.start_at>datetime.now(): return False
        
        if job.dependencies:
            for dep in job.dependencies:
                if dep not in self.completed:
                    return False
                
        return True
                
    @execution_controller
    def try_to_add_new_jobs_to_active(self):
        tmp = Queue()
        while not self.queue.empty():
            job = self.queue.get()
            if self.can_add_job_to_active(job):
                self.active_pool[job.name] = job
            else:
                tmp.put(job)
        while not tmp.empty():
            self.queue.put(tmp.get())

    @execution_controller
    def active_job_cycle(self):
        for k in self.active_pool:
            self.active_pool[k].run()

    #выгрузка задач в директорию
    @execution_controller
    def export_state(self):
        tmp = Queue()
        while not self.queue.empty():
            job = self.queue.get()
            JobLoader.save(job, self.directory_name)

        while not tmp.empty():
            self.queue.put(tmp.get())

        for job_name in self.active_pool:
            JobLoader.save(self.active_pool[job_name], self.directory_name)

        for job_name in self.completed:
            JobLoader.save(self.completed[job_name], self.directory_name)

    """
    подгрузка задач из директории. 
    поскольку планировщик не знает о внутренней структуре выполняемых внутри задач функций,
    сохранить их промежуточное состояние он не может, поэтому подгруженные функции будут выполняться
    с начала,
    """
    @execution_controller
    def load_state(self):
        loaded_jobs = JobLoader.load_from_dir(self.directory_name)
        for job in loaded_jobs:
            if job.status == JobStatus.CREATED:
                self.queue.put(job)
            if job.status == JobStatus.RUNNING:
                self.active_pool[job.name] = job
            if job.status == JobStatus.COMPLETED:
                self.completed[job.name] = job

    @execution_controller
    def restart(self):
        self.load_state()
        self.run()

    @execution_controller
    def stop(self):
        self.is_running = False
        self.loop_thread.join()
        self.export_state()


#работа с сетью
import requests


def get_request(num: int, out_queue, address = "http://ya.ru/", ) -> Generator[None, None, None]:
    logger.info("get_request started")
    for i in range(num):
        response = requests.get(address)
        assert response.status_code==200
        out_queue.put(response.text.strip())
        time.sleep(1)
        yield
    logger.info("get_request finished")

#работа с файловой системой
import requests


def mkdir(dirname) -> Generator[None, None, None]:
    logger.info("mkdir started")
    if not os.path.exists(dirname):
        os.mkdir(dirname) 
    yield
    logger.info("mkdir finished")

#работа с файлами
#в вечном цикле ждет входящих сообщений в очередь, затем сохраняет их в файл
import requests


def save2file(fout_name, queue_data) -> Generator[None, None, None]:
    file_counter = 0
    logger.info("save2file started")
    while True:
        if not queue_data.empty():
            data = queue_data.get()
            with open(fout_name+"_"+str(file_counter), 'a') as fout:
                fout.write(data+'\n')
            file_counter= file_counter+1
            time.sleep(3)
        yield
    logger.info("save2file finished")

#тестовая функция
def long_print_x(num: int, name: str) -> Generator[None, None, None]:
    for i in range(num):
        print(f'Вызов функции print_x с именем {name}')
        time.sleep(3)
        yield


"""
        Описание пайплайна: 
        1. Первая функция получает данные от сайта с погодой(по хорошему нужно передавать токен и получать json, но в рамках задачи пусть просто выгружает html)
        2. Вторая создает директорию для результатов
        3. Третья ждет, пока отработают первые две и сохраняет полученные результаты в файлы в заданной директории.
        """
if __name__ == '__main__':
    queue_data = Manager().Queue() #используется версия для мультипроцессов чтобы можно было сохранять через модуль pickle
    job_get_forecast = Job(target=get_request, args=(3, queue_data,"https://code.s3.yandex.net/async-module/moscow-response.json"), name = "get_forecast", start_at=datetime.now()+timedelta(seconds=10),tries=10)
    job_create_dir = Job(target=mkdir, args=('forecast_folder',), name = "mkdir_forecast")
    job_save_results = Job(target=save2file, args=('forecast_folder/yandex_forecast', queue_data), name = "save_forecast", dependencies=["get_forecast","mkdir_forecast"])


    sched = Scheduler()
    sched.schedule(job_save_results)
    sched.run()
    sched.stop()
    
    logger.info("sched sleep")
    time.sleep(5)
    logger.info("sched resumed")
    #тут зависнет, поскольку ждет выполнения других корутин
    sched.schedule(job_get_forecast)
    sched.schedule(job_create_dir)
    sched.restart()

    

    logger.info("finished")
    sched.stop()
    
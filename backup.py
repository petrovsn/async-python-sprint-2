from functools import wraps
from queue import Queue
from typing import Generator, Callable
from datetime import datetime, timedelta
from enum import Enum
from threading import Thread, Lock
import os
import time
import pickle
import glob
import requests

from modules.utils import execution_controller
# class syntax
class JobStatus(Enum):
    CREATED = 1
    RUNNING = 2
    COMPLETED = 3
    BROKEN = 4


class Job:
    @execution_controller
    def __init__(
        self, 
        target: Callable, 
        args: tuple | None = None, 
        kwargs: dict | None = None,
        name = "", start_at:datetime=None, max_working_time=-1, tries=0, dependencies=None ) -> None:
   
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.target = target
        self.coroutine = target(*self.args, **self.kwargs)

        self.name = name if name else str(id(self)) #уникальный идентификтор задачи
        self.start_at = start_at #запускается не раньше этого срока
        self.launched_at = None #фактическое время запуска
        self.max_working_time = None
        if max_working_time:
            if max_working_time>0:
                self.max_working_time = max_working_time
        self.remaining_tries = tries
        self.dependencies = dependencies if dependencies else []

        self.status = JobStatus.CREATED

    @execution_controller
    def run(self) -> None:
        if self.status==JobStatus.CREATED:
            self.status = JobStatus.RUNNING
            self.launched_at = datetime.now()

        if self.status != JobStatus.RUNNING: return
        if self.max_working_time:
            if datetime.now()>self.launched_at+timedelta(seconds=self.max_working_time):
                self.status = JobStatus.COMPLETED
        try:
            self.coroutine.send(None)

        except StopIteration:
            self.status = JobStatus.COMPLETED
        except Exception as e:
            print(self.name, repr(e), self.remaining_tries) 
            if self.remaining_tries:
                self.remaining_tries = self.remaining_tries-1
                self.coroutine = self.target(*self.args, **self.kwargs)
            else:
                self.status = JobStatus.BROKEN
                print(self.name, repr(e))

    @execution_controller
    def get_description(self):
        return {
            "name":self.name,
            "target": self.coroutine.__name__,
            "start_at": self.start_at,
            "status":self.status,
            "hash":None
        }
    

class JobLoader:
    @execution_controller
    def save(job:Job, directory_name):
        save_job = {
            "target":job.target,
            "name": job.name,
            "dependencies": job.dependencies,
            "args":job.args,
            "status":job.status,
            "start_at":job.start_at,
            "max_working_time": job.max_working_time,
            "tries": job.remaining_tries,
        }
        with  open(f"{directory_name+'/'+job.name}.pickle",'wb') as f_out:
            pickle.dump(save_job, f_out)

    @execution_controller
    def load(filename):
        save_job = {}
        with  open(filename, 'rb') as f_out:
            save_job = pickle.load(f_out)
        job = Job(target=save_job["target"], 
                  args=save_job["args"], 
                  name = save_job["name"], 
                  dependencies=save_job["dependencies"],
                  start_at=save_job["start_at"],
                  max_working_time=save_job["max_working_time"],
                  tries=save_job["tries"])
        job.status = save_job["status"]
        return job
    
    @execution_controller
    def load_from_dir(directory_name):
        job_saves = glob.glob(directory_name+"/*.pickle")
        results = []
        for job_save in job_saves:
            job = JobLoader.load(job_save)
            if job: results.append(job)
        return results
        

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
            if self.active_pool[name].status in [JobStatus.COMPLETED, JobStatus.BROKEN]:
                to_del.append(name)

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
    print("get_request started")
    for i in range(num):
        response = requests.get(address)
        assert response.status_code==200
        out_queue.put(response.text.strip())
        time.sleep(1)
        yield
    print("get_request finished")

#работа с файловой системой
import requests
def mkdir(dirname) -> Generator[None, None, None]:
    print("mkdir started")
    if os.path.exists(dirname):
       os.rmdir(dirname)
    os.mkdir(dirname) 
    yield
    print("mkdir finished")

#работа с файлами
import requests
def save2file(fout_name, queue_data) -> Generator[None, None, None]:
    file_counter = 0
    print("save2file started")
    while not queue_data.empty():
        data = queue_data.get()
        with open(fout_name+"_"+str(file_counter), 'a') as fout:
            fout.write(data+'\n')
        file_counter= file_counter+1
        time.sleep(3)
        yield
    print("save2file finished")

#тестовая функция
def long_print_x(num: int, name: str) -> Generator[None, None, None]:
    for i in range(num):
        print(f'Вызов функции print_x с именем {name}')
        time.sleep(3)
        yield


if __name__ == '__main__':
    queue_data = Queue()
    job_get_forecast = Job(target=get_request, args=(3, queue_data,"http://localhost:1924/"), name = "get_forecast", start_at=datetime.now()+timedelta(seconds=10),tries=10)
    job_create_dir = Job(target=mkdir, args=('forecast_folder',), name = "mkdir_forecast")
    job_save_results = Job(target=save2file, args=('forecast_folder/yandex_forecast', queue_data), name = "save_forecast", dependencies=["get_forecast"])


    sched = Scheduler()
    sched.schedule(job_get_forecast)
    sched.schedule(job_create_dir)
    sched.schedule(job_save_results)
    sched.run()
    
    input("finished")
    
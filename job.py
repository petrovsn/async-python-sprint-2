import glob
import pickle
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Generator

from modules.utils import execution_controller


#перечисление состояний
class JobStatus(Enum):
    CREATED = 1
    RUNNING = 2
    COMPLETED = 3
    BROKEN = 4


class Job:
    """
    справедливости ради, указывать в качестве параметра по умолчанию список, как в исходнике репозитория, 
    тоже не является безопасным решением, т.к. ведет к изменению поведения функции по мере записывания в эту
    переменную новых значений
    """
    # def __init__(self, start_at="", max_working_time=-1, tries=0, dependencies=[]):
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

        self.name = name if name else str(id(self)) #уникальный идентификтор задачи. 
        self.start_at = start_at #запускается не раньше этого срока
        self.launched_at = None #фактическое время запуска
        self.max_working_time = None
        if max_working_time:
            if max_working_time>0:
                self.max_working_time = max_working_time
        self.remaining_tries = tries
        self.dependencies = tuple(dependencies) if dependencies else ()

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
    
"""
Класс выгрузки и загрузки задач на диск. 
В качестве формата экспорта используется pickle, 
чтобы можно было подавать внутрь класса обертки Job
более широкий спектр корутин.
(следуемое из этого ограничение: все параметры должны быть pickable)
"""
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
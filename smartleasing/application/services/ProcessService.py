import traceback
from queue import Queue
from time import time

from injector import inject
from pdip.connection.models import DataQueueTask
from pdip.cqrs import Dispatcher
from pdip.dependency import IScoped
from pdip.dependency.container import DependencyContainer
from pdip.logging.loggers.database import SqlLogger
from pdip.processing import ProcessManager

from smartleasing.application.StartOperation.StartOperationCommand import StartOperationCommand
from smartleasing.application.StartOperation.StartOperationCommandRequest import ProcessInfo, \
    StartOperationCommandRequest


class ProcessService(IScoped):
    @inject
    def __init__(self,

                 dispatcher: Dispatcher,
                 logger: SqlLogger
                 ):

        self.dispatcher = dispatcher
        self.logger = logger

    @classmethod
    def start_source_operation(cls, sub_process_id, process_info, data_queue,
                               data_result_queue):
        return DependencyContainer \
            .Instance \
            .get(ProcessService) \
            .source_operation(sub_process_id, process_info, data_queue,
                              data_result_queue)

    def source_operation(self, sub_process_id, process_info: ProcessInfo, data_queue,
                         data_result_queue):
        try:
            transmitted_data_count = 0
            task_id = 0
            for process_info_detail in process_info.Details:
                task_id = task_id + 1
                data_queue_task = DataQueueTask(Id=task_id, Data={"process_info": process_info,
                                                                  "process_info_detail": process_info_detail},
                                                IsFinished=False)
                data_queue.put(data_queue_task)
                transmitted_data_count = transmitted_data_count + 1
                if transmitted_data_count >= process_info.ProcessCount:
                    result = data_result_queue.get()
                    if result:
                        transmitted_data_count = transmitted_data_count - 1
                    else:
                        break
            for i in range(process_info.ProcessCount):
                data_queue_finish_task = DataQueueTask(IsFinished=True)
                data_queue.put(data_queue_finish_task)
        except Exception as ex:
            for i in range(process_info.ProcessCount):
                data_queue_error_task = DataQueueTask(IsFinished=True, Traceback=traceback.format_exc(), Exception=ex)
                data_queue.put(data_queue_error_task)
            raise

    @classmethod
    def start_execute_operation(cls,
                                sub_process_id: int,
                                data_queue: Queue,
                                data_result_queue: Queue) -> int:
        return DependencyContainer \
            .Instance \
            .get(ProcessService) \
            .execute_operation(sub_process_id, data_queue, data_result_queue)

    def execute_operation(self,
                          sub_process_id: int,
                          data_queue: Queue,
                          data_result_queue: Queue) -> int:
        try:
            while True:
                data_task: DataQueueTask = data_queue.get()
                if data_task.IsFinished:
                    if data_task.Exception is not None:
                        exc = Exception(data_task.Traceback + '\n' + str(data_task.Exception))
                        raise exc
                    self.logger.info(f"{sub_process_id} process tasks finished")
                    return
                else:
                    start = time()
                    operation_data = data_task.Data
                    if operation_data is not None and "process_info" in operation_data and "process_info_detail" in operation_data:
                        region=operation_data['process_info'].Region
                        city=operation_data['process_info_detail'].City
                        if city is not None:
                            self.logger.info(
                                f"{sub_process_id}:{data_task.Id}-{region}-{city} process got a new task")
                            req = StartOperationCommandRequest(ProcessId=sub_process_id,
                                                               ProcessInfo=operation_data['process_info'],
                                                               Detail=operation_data['process_info_detail'])
                            command = StartOperationCommand(request=req)
                            self.dispatcher.dispatch(command)
                        else:
                            self.logger.info(
                                f"{sub_process_id}:{data_task.Id}-{region}-{city}  process got a new task")
                        end = time()
                        self.logger.info(
                            f"{sub_process_id}:{data_task.Id}-{region}-{city}  process finished task. time:{end - start}")
                    else:

                        end = time()
                        self.logger.info(
                            f"{sub_process_id}:{data_task.Id}  process finished task. time:{end - start}")
                    data_task.IsProcessed = True
                    data_result_queue.put(True)
        except Exception as ex:
            self.logger.exception(ex,
                f"{sub_process_id} subprocess getting error")
            data_result_queue.put(False)
            raise

    def start(self, process_info: ProcessInfo):
        if process_info.ProcessCount is not None and 1 < process_info.ProcessCount <= len(process_info.Details):
            try:
                source_data_process_manager = ProcessManager()
                execute_data_process_manager = ProcessManager()
                data_queue = source_data_process_manager.create_queue()
                data_result_queue = source_data_process_manager.create_queue()

                source_data_kwargs = {
                    "process_info": process_info,
                    "data_queue": data_queue,
                    "data_result_queue": data_result_queue,
                }

                source_data_process_manager.start_processes(
                    process_count=1,
                    target_method=self.start_source_operation,
                    kwargs=source_data_kwargs)

                execute_data_kwargs = {
                    "data_queue": data_queue,
                    "data_result_queue": data_result_queue,
                }

                execute_data_process_manager.start_processes(
                    process_count=process_info.ProcessCount,
                    target_method=self.start_execute_operation,
                    kwargs=execute_data_kwargs)

                execute_data_process_results = execute_data_process_manager.get_results()
            except Exception as ex:
                self.logger.error(f"Integration getting error: {ex}, traceback: {traceback.format_exc()}")
                raise
            finally:
                del source_data_process_manager
                del execute_data_process_manager
        else:
            for process_info_detail in process_info.Details:
                req = StartOperationCommandRequest(ProcessId=1,
                                                   ProcessInfo=process_info,
                                                   Detail=process_info_detail)
                command = StartOperationCommand(request=req)
                self.dispatcher.dispatch(command)

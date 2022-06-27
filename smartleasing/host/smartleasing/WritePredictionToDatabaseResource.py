from datetime import datetime

from flask import request
from injector import inject
from pdip.api.base import ResourceBase
from pdip.logging.loggers.database import SqlLogger

from smartleasing.application.WritePredictionToDatabase.WritePredictionToDatabaseCommand import \
    WritePredictionToDatabaseCommand
from smartleasing.application.WritePredictionToDatabase.WritePredictionToDatabaseCommandRequest import \
    WritePredictionToDatabaseCommandRequest
from smartleasing.application.scheduler.JobScheduler import JobScheduler
from smartleasing.application.scheduler.JobService import JobService


class WritePredictionToDatabaseResource(ResourceBase):
    @inject
    def __init__(self,
                 job_scheduler: JobScheduler,
                 logger: SqlLogger,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logger
        self.job_scheduler = job_scheduler

    def post(self, req: WritePredictionToDatabaseCommandRequest):
        self.logger.debug(f"request data: {request.data}")
        command = WritePredictionToDatabaseCommand(request=req)
        if command.request.RunDate is not None and command.request.RunDate != '':
            job_run_date = datetime.strptime(command.request.RunDate, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone()
        else:
            job_run_date = datetime.now().astimezone()

        job = self.job_scheduler.add_job_with_date(job_function=JobService.job_start,
                                                   run_date=job_run_date,
                                                   args=(command,))

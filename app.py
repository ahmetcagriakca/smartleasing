import os

from apscheduler.triggers.cron import CronTrigger
from flask_restx import Api
from pdip.api.app import FlaskAppWrapper
from pdip.base import Pdi
from pdip.cqrs.dispatcher import Dispatcher
from pdip.io import FolderManager
from smartleasing.application.CreateProcessData.CreateProcessData import CreateProcessDataCommand
from smartleasing.application.CreateProcessData.CreateProcessDataCommandRequest import CreateProcessDataCommandRequest

from smartleasing.application.StartAll.StartAllCommandRequest import StartAllCommandRequest
from smartleasing.application.StartAll.StartAllCommand import StartAllCommand
from smartleasing.application.scheduler.JobScheduler import JobScheduler
from smartleasing.application.scheduler.JobService import JobService
from smartleasing.domain.configs.ProcessConfig import ProcessConfig
from smartleasing.host.smartleasing.FileResource import FileResource

if __name__ == '__main__':
    def startup(binder):
        api = binder.injector.get(Api)
        api.add_resource(FileResource, '/api/Smartleasing/File')


    pdi = Pdi(excluded_modules=['files','env'], configurations=[startup])
    folder = os.path.join(pdi.root_directory, 'files', 'db')
    pdi.get(FolderManager).create_folder_if_not_exist(folder)


    # from smartleasing.domain.base.base import Base
    # from pdip.data import DatabaseSessionManager
    # engine = pdi.get(DatabaseSessionManager).engine
    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine)
    def remove_all_jobs(job_scheduler):
        all_jobs = job_scheduler.get_jobs()
        for cron_job in all_jobs:
            job_scheduler.remove_job(cron_job.id)


    def initialize_crons(job_scheduler):
        remove_all_jobs(job_scheduler)
        process_config = pdi.get(ProcessConfig)
        region = 'ALL'
        cities = ['Adana', 'Adıyaman', 'Afyonkarahisar', 'Ağrı', 'Aksaray', 'Amasya', 'Ankara', 'Antalya', 'Ardahan',
                  'Artvin', 'Aydın', 'Balıkesir', 'Bartın', 'Batman', 'Bayburt', 'Bilecik', 'Bingöl', 'Bitlis', 'Bolu',
                  'Burdur', 'Bursa', 'Çanakkale', 'Çankırı', 'Çorum', 'Denizli', 'Diyarbakır', 'Düzce', 'Edirne',
                  'Elazığ', 'Erzincan', 'Erzurum', 'Eskişehir', 'Gaziantep', 'Giresun', 'Gümüşhane', 'Hakkari', 'Hatay',
                  'Iğdır', 'Isparta', 'İstanbul', 'İzmir', 'Kahramanmaraş', 'Karabük', 'Karaman', 'Kars', 'Kastamonu',
                  'Kayseri', 'Kilis', 'Kırıkkale', 'Kırklareli', 'Kırşehir', 'Kocaeli', 'Konya', 'Kütahya', 'Malatya',
                  'Manisa', 'Mardin', 'Mersin', 'Muğla', 'Muş', 'Nevşehir', 'Niğde', 'Ordu', 'Osmaniye', 'Rize',
                  'Sakarya', 'Samsun', 'Şanlıurfa', 'Siirt', 'Sinop', 'Şırnak', 'Sivas', 'Tekirdağ', 'Tokat', 'Trabzon',
                  'Tunceli', 'Uşak', 'Van', 'Yalova', 'Yozgat', 'Zonguldak']
        req = StartAllCommandRequest()
        req.ProcessCount = int(process_config.process_count)

        req.Region = region
        req.Cities = cities

        req.RecreateBuildingCsv = True
        req.RecreateLeasingCsv = True
        req.FindAgainAllData = True

        req.TruncateBaseStationTable= True
        req.TruncatePredictionTable = True
        req.WritePredictionToDatabase = True
        req.WriteBaseStationToDatabase = True

        command = StartAllCommand(request=req)
        trigger = CronTrigger.from_crontab("0 0 1 * *")
        job = job_scheduler.add_job_with_cron(job_function=JobService.job_start,
                                              cron=trigger,
                                              args=(command,))

    def start():
        job_scheduler = pdi.get(JobScheduler)
        job_scheduler.run()
        initialize_crons(job_scheduler)
        pdi.get(FlaskAppWrapper).run()
    def get_leasing_data():
        command = CreateProcessDataCommand(CreateProcessDataCommandRequest(RecreateLeasingCsv=True))
        dispatcher = pdi.get(Dispatcher)
        dispatcher.dispatch(command)
    start()
import os

import pandas as pd
from injector import inject
from pdip.configuration.models.application import ApplicationConfig
from pdip.connection.database.base import DatabaseProvider
from pdip.dependency import IScoped
from pdip.dependency.container import DependencyContainer
from pdip.io import FolderManager
from pdip.logging.loggers.database import SqlLogger
from pdip.processing import ProcessManager

from smartleasing.application.services.DataService import DataService
from smartleasing.domain.configs.BuildingConfig import BuildingConfig
from smartleasing.domain.configs.OneMapDatabaseConfig import OneMapDatabaseConfig
from smartleasing.domain.utils import Utils


class BuildingService(IScoped):
    @inject
    def __init__(self,
                 application_config: ApplicationConfig,
                 building_config: BuildingConfig,
                 logger: SqlLogger,
                 one_map_database_config: OneMapDatabaseConfig,
                 database_provider: DatabaseProvider,
                 folder_manager: FolderManager,
                 data_service: DataService,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_service = data_service
        self.building_config = building_config
        self.folder_manager = folder_manager
        self.application_config = application_config
        self.logger = logger
        self.database_provider = database_provider
        self.one_map_database_config = one_map_database_config
        self.one_map_database_context = self.database_provider.get_context_by_config(
            config=self.one_map_database_config)

    def check_bina_3d_null_data(self):
        check_query = f'SELECT * FROM (SELECT 1 FROM {self.building_config.base_table_name} WHERE LATITUDE is null or LONGITUDE is NULL) WHERE ROWNUM=1 '
        # check_query = f'SELECT 1 FROM {self.building_config.base_table_name} WHERE ROWNUM=1'
        count = self.one_map_database_context.get_table_count(check_query)
        return count > 0

    def check_building_detail_data(self):
        check_query = f'SELECT 1 FROM {self.building_config.building_detail_table_name} WHERE ROWNUM=1'
        count = self.one_map_database_context.get_table_count(check_query)
        return count > 0

    def check_building_data(self):
        check_query = f'SELECT 1 FROM {self.building_config.building_table_name} WHERE ROWNUM=1'
        # check_query = f'SELECT * FROM (SELECT 1 FROM {self.building_config.building_table_name} WHERE LATITUDE is null or LONGITUDE is NULL) WHERE ROWNUM=1 '
        count = self.one_map_database_context.get_table_count(check_query)
        return count > 0

    def truncate_building_detail_data(self):
        truncate_building_detail_data_script = f'delete from {self.building_config.building_detail_table_name}'
        self.one_map_database_context.execute(
            truncate_building_detail_data_script)

    def create_building_detail_data_with_location(self):
        create_building_detail_data_script = f'''
MERGE INTO {self.building_config.building_detail_table_name} d
USING (
SELECT
tt.sirano, tt.tip_adi, tt.kat, tt.yukseklik, tt.alanm2, nvl(tt.longitude, t.x) AS longitude, nvl(tt.latitude, t.y) AS latitude, tt.basarid, tt.mahalle_adi, tt.il_adi, tt.ilce_adi, tt.mi_style, tt.mi_prinx
FROM {self.building_config.base_table_name} tt, TABLE(sdo_util.getvertices(sdo_cs.transform(tt.geoloc, 8199))) t
WHERE t.id = 1
) i
--#161452
ON
(d.mi_prinx = i.mi_prinx)
WHEN NOT MATCHED THEN
INSERT
(
sirano, tip_adi, kat, yukseklik, alanm2, longitude, latitude, basarid, mahalle_adi, il_adi, ilce_adi, mi_style, mi_prinx
)
VALUES
(
i.sirano, i.tip_adi, i.kat, i.yukseklik, i.alanm2, i.longitude, i.latitude, i.basarid, i.mahalle_adi, i.il_adi, i.ilce_adi, i.mi_style, i.mi_prinx
)
'''
        self.one_map_database_context.execute(
            create_building_detail_data_script)

    def create_building_detail_data(self):
        create_building_detail_data_script = f'''
MERGE INTO {self.building_config.building_detail_table_name} d
	USING (
	SELECT
	tt.sirano, tt.tip_adi, tt.kat, tt.yukseklik, tt.alanm2, tt.longitude, tt.latitude, tt.basarid, tt.mahalle_adi, tt.il_adi, tt.ilce_adi, tt.mi_style, tt.mi_prinx
FROM
	{self.building_config.base_table_name} tt
	) i
--#161452
	ON
(d.mi_prinx = i.mi_prinx)
WHEN NOT MATCHED THEN
	INSERT
	(
	sirano, tip_adi, kat, yukseklik, alanm2, longitude, latitude, basarid, mahalle_adi, il_adi, ilce_adi, mi_style, mi_prinx
	)
VALUES
	(
	i.sirano, i.tip_adi, i.kat, i.yukseklik, i.alanm2, i.longitude, i.latitude, i.basarid, i.mahalle_adi, i.il_adi, i.ilce_adi, i.mi_style, i.mi_prinx
	)
        '''
        self.one_map_database_context.execute(
            create_building_detail_data_script)

    def truncate_building_data(self):
        truncate_building_data_script = f'delete from {self.building_config.building_table_name}'
        self.one_map_database_context.execute(truncate_building_data_script)

    def get_building_detail_count(self):
        count = self.one_map_database_context.get_table_count(
            query=f"SELECT * FROM {self.building_config.building_detail_table_name}")
        return count

    def get_bina_3d_count(self):
        count = self.one_map_database_context.get_table_count(
            query=f"SELECT * FROM {self.building_config.base_table_name}")
        return count

    def create_building_detail_data_with_location_start(self):
        count = self.get_bina_3d_count()
        process_count = int(self.building_config.building_process_count)
        limit = (int(count / process_count) + 10000)
        data_kwargs = {
            "limit": limit
        }
        process_manager = ProcessManager()
        try:
            process_manager.start_processes(
                process_count=process_count,
                target_method=self.start_create_building_detail_data_with_location_parallel,
                kwargs=data_kwargs)
            results = process_manager.get_results()
            for result in results:
                assert result.State == 3
        finally:
            if process_manager is not None:
                del process_manager

    @classmethod
    def start_create_building_detail_data_with_location_parallel(cls, sub_process_id, limit):
        return DependencyContainer \
            .Instance \
            .get(BuildingService) \
            .create_building_detail_data_with_location_parallel(sub_process_id, limit)

    def create_building_detail_data_with_location_parallel(self, page, limit):
        v_start = (page - 1) * limit
        v_end = page * limit
        create_building_detail_data_script = f'''
declare
    v_start number:={v_start};
    v_end number:={v_end};
    v_count number;
    v_total_area number;
        cursor cur is
            WITH TEMP_INTEGRATION AS(SELECT ordered_query.*,ROW_NUMBER() OVER ( order by MI_PRINX) row_number FROM (
            SELECT
                tt.sirano, tt.tip_adi, tt.kat, tt.yukseklik, tt.alanm2, nvl(tt.longitude, t.x) AS longitude, nvl(tt.latitude, t.y) AS latitude, tt.basarid, tt.mahalle_adi, tt.il_adi, tt.ilce_adi, tt.mi_style, tt.mi_prinx
            FROM {self.building_config.base_table_name} tt, TABLE(sdo_util.getvertices(sdo_cs.transform(tt.geoloc, 8199))) t
            WHERE t.id = 1) ordered_query) SELECT * FROM TEMP_INTEGRATION WHERE row_number >= v_start AND row_number < v_end;
begin
    v_count := 0;

    for rec in cur
    loop
        v_count := v_count + 1;

        insert into {self.building_config.building_detail_table_name} 
        (sirano, tip_adi, kat, yukseklik, alanm2, longitude, latitude, basarid, mahalle_adi, il_adi, ilce_adi, mi_style, mi_prinx)
        values
        ( rec.sirano, rec.tip_adi, rec.kat, rec.yukseklik, rec.alanm2, rec.longitude, rec.latitude, rec.basarid, rec.mahalle_adi, rec.il_adi, rec.ilce_adi, rec.mi_style, rec.mi_prinx);
        if v_count > 50000 then
            commit;
            v_count := 0;
        end if;
    end loop;
    commit;
end;
            '''
        self.logger.debug(f'{page} started with {limit}.')
        self.logger.debug(f'query:{create_building_detail_data_script}')

        row_count = self.one_map_database_context.execute(
            create_building_detail_data_script)
        self.logger.debug(
            f'{page} finisihed with {limit}. Affected Row Count : {row_count}')

    def create_building_data(self):
        count = self.get_building_detail_count()
        process_count = int(self.building_config.building_process_count)
        limit = (int(count / process_count) + 10000)
        data_kwargs = {
            "limit": limit
        }
        process_manager = ProcessManager()
        try:
            process_manager.start_processes(
                process_count=process_count,
                target_method=self.start_create_building_data_parallel,
                kwargs=data_kwargs)
            results = process_manager.get_results()

            for result in results:
                assert result.State == 3
        finally:
            if process_manager is not None:
                del process_manager

    @classmethod
    def start_create_building_data_parallel(cls, sub_process_id, limit):
        return DependencyContainer \
            .Instance \
            .get(BuildingService) \
            .create_building_data_parallel(sub_process_id, limit)

    def create_building_data_parallel(self, page, limit):
        v_start = (page - 1) * limit
        v_end = page * limit
        create_building_data_script = f'''
declare
    v_start number:={v_start};
    v_end number:={v_end};
    v_count number;
    v_total_area number;
        cursor cur is
            WITH TEMP_INTEGRATION AS(SELECT ordered_query.*,ROW_NUMBER() OVER ( order by MI_PRINX) row_number FROM (select tt.sirano,tt.tip_adi,tt.kat,tt.yukseklik,tt.alanm2,tt.longitude,tt.latitude, tt.basarid,tt.mahalle_adi,tt.il_adi,tt.ilce_adi,tt.mi_style,tt.mi_prinx
            from {self.building_config.building_detail_table_name} tt ) ordered_query) SELECT * FROM TEMP_INTEGRATION WHERE row_number >= v_start AND row_number < v_end;
begin
    v_count := 0;

    for rec in cur
    loop
        v_count := v_count + 1;

        SELECT sum(t.alanm2)
        INTO v_total_area
        FROM {self.building_config.building_detail_table_name} t
        WHERE t.latitude < rec.latitude + 0.001348982409
        and t.latitude > rec.latitude - 0.001348982409
        and t.longitude < rec.longitude + 0.001786544172
        and t.longitude > rec.longitude - 0.001786544172;
        insert into {self.building_config.building_table_name} (sirano, tip_adi, kat, yukseklik, alanm2, longitude, latitude, basarid, mahalle_adi, il_adi, ilce_adi, mi_style, mi_prinx, total_alan)
        values( rec.sirano, rec.tip_adi, rec.kat, rec.yukseklik, rec.alanm2, rec.longitude, rec.latitude, rec.basarid, rec.mahalle_adi, rec.il_adi, rec.ilce_adi, rec.mi_style, rec.mi_prinx, v_total_area);
        if v_count > 50000 then
            commit;
            v_count := 0;
        end if;
    end loop;
    commit;
end;
            '''
        self.logger.debug(f'{page} started with {limit}.')
        self.logger.debug(f'query:{create_building_data_script}')

        row_count = self.one_map_database_context.execute(
            create_building_data_script)
        self.logger.debug(
            f'{page} finisihed with {limit}. Affected Row Count : {row_count}')

    def write_to_csv(self, path, data, mode='w', has_header=True):
        data.TIP_ADI = data.TIP_ADI.str.replace('INDUSTRIAL & COMMERCIA',
                                                'INDUSTRIAL&COMMERCIAL')
        data.TIP_ADI = data.TIP_ADI.str.replace('GREEN HOUSE', 'GREENHOUSE')
        data.TIP_ADI = data.TIP_ADI.str.replace('CITY WALLS', 'CITY_WALLS')
        data.to_csv(path, mode=mode, header=has_header, index=False, sep=';', decimal=',')

    def write_building_data_to_csv(self, region, city, recreate=False):
        unicode_city_name = Utils.translate(city)
        folder = os.path.join(self.application_config.root_directory, 'files', 'data')
        self.folder_manager.create_folder_if_not_exist(folder)
        path = os.path.join(folder, f'building-{region}-{unicode_city_name}.csv')
        self.logger.debug(f'{region}-{unicode_city_name} started')

        if not os.path.exists(path) or recreate:
            self.logger.debug(f'all data started')
            if self.building_config.building_query is None:
                building_data_query = f'''
select * from (SELECT
tt.sirano, tt.tip_adi, tt.kat, tt.yukseklik, tt.alanm2, nvl(tt.longitude, t.x) AS longitude, nvl(tt.latitude, t.y) AS latitude, tt.basarid, tt.mahalle_adi, tt.il_adi, tt.ilce_adi, tt.mi_style, tt.mi_prinx
FROM {self.building_config.base_table_name} tt, TABLE(sdo_util.getvertices(sdo_cs.transform(tt.geoloc, 8199))) t
WHERE t.id = 1
) x 
where IL_ADI in ('{city}')
'''
            else:
                building_data_query = self.building_config.building_query + f" where IL_ADI in ('{unicode_city_name}')"
            # SELECT * FROM {self.building_config.building_detail_table_name} where IL_ADI in ({cities_str})'
            self.one_map_database_context.connector.connect()
            if os.path.exists(path):
                os.remove(path)
            count = 0
            chunks = pd.read_sql(building_data_query, con=self.one_map_database_context.connector.connection,
                                 chunksize=50000)
            chunk = next(chunks)
            count += 1
            self.logger.debug(f'Building data reading from db. count:{count}')
            self.write_to_csv(path, data=chunk)
            self.logger.debug(f'Building data writed to csv. count:{count}')
            for chunk in chunks:
                count += 1
                self.logger.debug(f'Building data reading from db. count:{count}')
                self.write_to_csv(path, data=chunk, mode='a', has_header=False)
                self.logger.debug(f'Building data writed to csv. count:{count}')
            self.one_map_database_context.connector.disconnect()
            self.logger.debug(f'{region}-{unicode_city_name} finished')

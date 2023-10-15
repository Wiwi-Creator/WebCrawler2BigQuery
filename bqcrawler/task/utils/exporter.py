import time
import logging
from pytz import timezone, utc
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import Any, BinaryIO, Dict, Iterable, List, Union


class BigqueryExporterBase:
    def __init__(self, projectID: str):
        self.projectID = projectID
        self.bq_client = bigquery.Client(project=projectID)

    def table_rows_count(self, datasetID: str, tableID: str):
        query = f"SELECT 1 FROM `{self.projectID}.{datasetID}.{tableID}`"
        query_job = self.bq_client.query(query)
        results = query_job.result()
        return results.total_rows

    def is_table_primary_key(self, datasetID: str, tableID: str, primary_keys: List[str] = []):
        total_rows = self.table_rows_count(datasetID, tableID)

        key_where_condition = ' AND '.join([f'{key} IS NOT NULL' for key in primary_keys])
        query = (
            f"""
            SELECT COUNT(*) AS distinct_rows
            FROM (
                SELECT DISTINCT {', '.join(primary_keys)}
                FROM `{self.projectID}.{datasetID}.{tableID}` WHERE {key_where_condition}
            )"""
        )
        query_job = self.bq_client.query(query)
        distinct_rows = query_job.result()
        distinct_rows = next(iter(distinct_rows))[0]
        if total_rows != distinct_rows:
            return False
        else:
            return True

    def drop_table(self, datasetID: str, tableID: str):
        logging.info(f'Drop table: {self.projectID}.{datasetID}.{tableID}')
        self.bq_client.delete_table(f'{self.projectID}.{datasetID}.{tableID}', not_found_ok=True)

    def is_exist_table(self, datasetID: str, tableID: str):
        try:
            self.bq_client.get_table(f'{self.projectID}.{datasetID}.{tableID}')
            return True
        except NotFound:
            return False

    def _delete_add_schema_migo_fetch_datetime(self, schema_path: Union[str, BinaryIO]):
        """ Delete schema configs containing 'migo_fetch_datetime' and add it"""
        schema = self.bq_client.schema_from_json(schema_path)
        schema_result = [field for field in schema if field.name != 'migo_fetch_datetime']
        schema_result.append(bigquery.SchemaField(name="migo_fetch_datetime", field_type="DATETIME", mode="NULLABLE", description="MIGO獲取資料時間"))

        return schema_result

    def _add_migo_fetch_datetime(
        self,
        json_array: List[dict]
    ):
        current_datetime = datetime.utcnow().replace(tzinfo=utc).astimezone(timezone('Asia/Taipei')).strftime("%Y-%m-%d %H:%M:%S")

        for json_item in json_array:
            json_item.update({'migo_fetch_datetime': current_datetime})

        return json_array

    def create_table(self, schema_path: Union[str, BinaryIO], datasetID: str, tableID: str):
        if self.is_exist_table(datasetID, tableID):
            logging.warning(f"{self.projectID}.{datasetID}.{tableID} is already exists")
        else:
            logging.info('create %s.%s.%s' % (self.projectID, datasetID, tableID))
            table = bigquery.Table(
                f'{self.projectID}.{datasetID}.{tableID}',
                schema=self._delete_add_schema_migo_fetch_datetime(schema_path)
            )
            table = self.bq_client.create_table(table)

    def load_table_using_replace(
        self,
        data: Iterable[Dict[str, Any]],
        schema_path: Union[str, BinaryIO],
        datasetID: str,
        tableID: str
    ):
        """ Only used to upload data to TEMP TABLE in the while loop, not used to update the TARGET table"""
        try:
            data_with_migo_fetch_datetime = self._add_migo_fetch_datetime(data)
            self.create_table(schema_path, datasetID, tableID)

            job_config = bigquery.LoadJobConfig()
            job_config.ignore_unknown_values = True
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job_config.schema = self._delete_add_schema_migo_fetch_datetime(schema_path)
            self.bq_client.load_table_from_json(
                data_with_migo_fetch_datetime,
                project=self.projectID,
                destination=f'{datasetID}.{tableID}',
                job_config=job_config,
            ).result()

        except Exception as err:
            raise Exception(err)

    def update_table_using_replace(
        self,
        data: Iterable[Dict[str, Any]],
        schema_path: Union[str, BinaryIO],
        datasetID: str,
        tableID: str
    ):
        try:
            temp_table_id = f"{tableID}_{round(time.time())}"
            self.load_table_using_replace(data, schema_path, datasetID, temp_table_id)

            temp_table_row_count = self.table_rows_count(datasetID, temp_table_id)
            if temp_table_row_count == len(data):
                self.create_table(schema_path, datasetID, tableID)

                job_config = bigquery.CopyJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                job = self.bq_client.copy_table(
                    f'{self.projectID}.{datasetID}.{temp_table_id}',
                    f'{self.projectID}.{datasetID}.{tableID}',
                    job_config=job_config
                )
                job.result()
            else:
                raise Exception("temp table row count is different from local data")
        except Exception as err:
            raise Exception(err)
        finally:
            self.drop_table(datasetID, temp_table_id)

    def update_table_using_delete_insert(
        self,
        data: Iterable[Dict[str, Any]],
        schema_path: Union[str, BinaryIO],
        datasetID: str,
        tableID: str,
        mapping_fields: List[str]
    ):
        """ Only used to update from TEMP TABLE to TARGET TABLE """
        try:
            temp_table_id = f"{tableID}_{round(time.time())}"
            self.load_table_using_replace(data, schema_path, datasetID, temp_table_id)

            temp_table_row_count = self.table_rows_count(datasetID, temp_table_id)
            if temp_table_row_count == len(data):
                self.create_table(schema_path, datasetID, tableID)
                key_where_condition = ' AND '.join([f' base.{key} = ref.{key} ' for key in mapping_fields])
                self.bq_client.query(
                    query=f"""
                        DELETE
                        `{self.projectID}.{datasetID}.{tableID}` AS base
                        WHERE
                        EXISTS(
                            SELECT 1 FROM `{self.projectID}.{datasetID}.{temp_table_id}` AS ref
                            WHERE {key_where_condition}
                        )
                        """,
                    project=self.projectID
                ).result()

                job_config = bigquery.CopyJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job = self.bq_client.copy_table(
                    f'{self.projectID}.{datasetID}.{temp_table_id}',
                    f'{self.projectID}.{datasetID}.{tableID}',
                    job_config=job_config
                )
                job.result()
            else:
                raise Exception("temp table row count is different from local data")
        except Exception as err:
            raise Exception(err)
        finally:
            self.drop_table(datasetID, temp_table_id)

    def update_table_using_append(
        self,
        data: Iterable[Dict[str, Any]],
        schema_path: Union[str, BinaryIO],
        datasetID: str,
        tableID: str
    ):
        """ Only used to update from TEMP TABLE to TARGET TABLE """
        try:
            temp_table_id = f"{tableID}_{round(time.time())}"
            self.load_table_using_replace(data, schema_path, datasetID, temp_table_id)

            temp_table_row_count = self.table_rows_count(datasetID, temp_table_id)
            if temp_table_row_count == len(data):
                self.create_table(schema_path, datasetID, tableID)

                job_config = bigquery.CopyJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job = self.bq_client.copy_table(
                    f'{self.projectID}.{datasetID}.{temp_table_id}',
                    f'{self.projectID}.{datasetID}.{tableID}',
                    job_config=job_config
                )
                job.result()
            else:
                raise Exception("temp table row count is different from local data")
        except Exception as err:
            raise Exception(err)
        finally:
            self.drop_table(datasetID, temp_table_id)
            
    def streaming_insert(
        self,
        data,
        datasetID: str,
        tableID: str
    ):
        """ Insert Into a TABLE """
        data_with_migo_frtch_time = self._add_migo_fetch_datetime(data)
        table_ref = self.bq_client.dataset(datasetID).table(tableID)
        error = self.bq_client.insert_rows_json(table_ref, data_with_migo_frtch_time)
        if not error:
            logging.info(f'Inserted {len(data)} referees detail into table {self.projectID}.{datasetID}.{tableID}')
        else:
            logging.info("Inserted Fail , Reason: ", error)


if __name__ == '__main__':
    BigqueryExporterBase.__init__()
import logging
from bq2lit.core.postgresql.validator import PostgresqlValidator


class LitTaskValidator(object):
    @staticmethod
    def validate_brand_info(env: str, company: str, brand: str, brand_id: int):
        logging.info(f"validate brand_id: {brand_id} related info")
        validator = PostgresqlValidator(
            env=env
        )
        validator.validate_brand_id_exists(
            company=company, brand=brand, brand_id=brand_id
        )
        logging.info(f"validate brand_id: {brand_id} related info complete")

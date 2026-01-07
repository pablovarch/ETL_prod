# domain_attributes_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomainAttributesSync:
    """
    Sincroniza ciertos campos de public.domain_attributes
    desde scraping_db hacia prod_db usando processed/processed_at.

    Campos sincronizados:
      - domain_classification_id
      - online_status
      - offline_type
      - site_url
      - status_msg
      - updated_by
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("domain_attributes_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None

    def connect(self):
        self.logger.info("Iniciando conexiones (domain_attributes_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (domain_attributes_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (domain_attributes_sync)")
        try:
            if self.scraping_conn:
                self.scraping_conn.close()
            if self.prod_conn:
                self.prod_conn.close()
        except Exception as e:
            self.logger.exception(f"Error al cerrar conexiones: {e}")

    def fetch_pending_rows(self, limit: int):
        self.logger.info(
            f"[domain_attributes_sync] Buscando filas processed=false (limite={limit})"
        )
        query = """
            SELECT
                domain_id,
                domain_classification_id,
                online_status,
                offline_type,
                site_url,
                status_msg,
                updated_by
            FROM public.domain_attributes
            WHERE processed = false
            ORDER BY domain_id
            LIMIT %s
        """
        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()
        self.logger.info(
            f"[domain_attributes_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def apply_update_in_prod(self, row: dict) -> int:
        self.logger.debug(
            f"[domain_attributes_sync] UPDATE prod para domain_id={row['domain_id']}"
        )
        query = """
            UPDATE public.domain_attributes
            SET domain_classification_id = %s,
                online_status            = %s,
                offline_type             = %s,
                site_url                 = %s,
                status_msg               = %s,
                updated_by               = %s
            WHERE domain_id = %s
        """
        params = (
            row["domain_classification_id"],
            row["online_status"],
            row["offline_type"],
            row["site_url"],
            row["status_msg"],
            row["updated_by"],
            row["domain_id"],
        )
        with self.prod_conn.cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount

    def mark_as_processed(self, domain_id: int):
        self.logger.debug(
            f"[domain_attributes_sync] Marcando processed=true para domain_id={domain_id}"
        )
        query = """
            UPDATE public.domain_attributes
            SET processed = true,
                processed_at = NOW()
            WHERE domain_id = %s
        """
        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (domain_id,))

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)
        if not rows:
            self.logger.info(
                "[domain_attributes_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[domain_attributes_sync] Procesando batch de {len(rows)} dominios"
        )
        try:
            for row in rows:
                affected = self.apply_update_in_prod(row)
                if affected == 0:
                    self.logger.warning(
                        f"[domain_attributes_sync] Ninguna fila actualizada en prod "
                        f"para domain_id={row['domain_id']}"
                    )
                else:
                    self.logger.debug(
                        f"[domain_attributes_sync] UPDATE aplicado en prod para "
                        f"domain_id={row['domain_id']}"
                    )
                self.mark_as_processed(row["domain_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.logger.info(
                f"[domain_attributes_sync] Batch OK ({len(rows)} dominios procesados)"
            )
        except Exception as e:
            self.logger.exception(
                f"[domain_attributes_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()
        return len(rows)

    def run(self):
        self.logger.info("===== Inicio ETL domain_attributes_sync =====")
        self.connect()
        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed
            self.logger.info(
                f"ETL domain_attributes_sync finalizado. Total dominios: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL domain_attributes_sync =====")

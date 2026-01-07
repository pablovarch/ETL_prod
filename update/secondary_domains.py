# secondary_domains_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class SecondaryDomainsSync:
    """
    Sincroniza public.secondary_domains desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.secondary_domains.
      - UPSERT en prod.secondary_domains usando ON CONFLICT (sec_domain_id).
      - Marca processed = true, processed_at = NOW() en scraping.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("secondary_domains_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (secondary_domains_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (secondary_domains_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (secondary_domains_sync)")
        try:
            if self.scraping_conn:
                self.scraping_conn.close()
            if self.prod_conn:
                self.prod_conn.close()
        except Exception as e:
            self.logger.exception(f"Error al cerrar conexiones: {e}")

    # ---------- Operaciones de BD ----------

    def fetch_pending_rows(self, limit: int):
        """
        Filas pendientes en scraping.secondary_domains (processed = false).
        """
        self.logger.info(
            f"[secondary_domains_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                sec_domain_id,
                redirect_domain,
                html_length,
                online_status
            FROM public.secondary_domains
            WHERE processed = false
            ORDER BY sec_domain_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[secondary_domains_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.secondary_domains usando sec_domain_id como clave.
        """
        self.logger.debug(
            f"[secondary_domains_sync] Upsert prod para sec_domain_id={row.get('sec_domain_id')}"
        )

        query = """
            INSERT INTO public.secondary_domains (
                sec_domain_id,
                redirect_domain,
                html_length,
                online_status
            ) VALUES (
                %(sec_domain_id)s,
                %(redirect_domain)s,
                %(html_length)s,
                %(online_status)s
            )
            ON CONFLICT (sec_domain_id) DO UPDATE SET
                redirect_domain = EXCLUDED.redirect_domain,
                html_length     = EXCLUDED.html_length,
                online_status   = EXCLUDED.online_status;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, sec_domain_id: int):
        """
        Marca processed=true en scraping.secondary_domains.
        """
        self.logger.debug(
            f"[secondary_domains_sync] Marcando processed=true para sec_domain_id={sec_domain_id}"
        )

        query = """
            UPDATE public.secondary_domains
            SET processed    = true,
                processed_at = NOW()
            WHERE sec_domain_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (sec_domain_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)
        if not rows:
            self.logger.info(
                "[secondary_domains_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[secondary_domains_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["sec_domain_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.logger.info(
                f"[secondary_domains_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[secondary_domains_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL secondary_domains_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL secondary_domains_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL secondary_domains_sync =====")

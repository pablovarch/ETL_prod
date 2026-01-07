# secondary_domains_html_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class SecondaryDomainsHtmlSync:
    """
    Sincroniza public.secondary_domains_html desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.secondary_domains_html.
      - UPSERT en prod.secondary_domains_html usando ON CONFLICT(sec_domain_html_id).
      - Marca processed = true, processed_at = NOW() en scraping.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("secondary_domains_html_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (secondary_domains_html_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (secondary_domains_html_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (secondary_domains_html_sync)")
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
        Filas pendientes en scraping.secondary_domains_html (processed = false).
        """
        self.logger.info(
            f"[secondary_domains_html_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                sec_domain_html_id,
                html_content,
                sec_domain_id
            FROM public.secondary_domains_html
            WHERE processed = false
            ORDER BY sec_domain_html_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[secondary_domains_html_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.secondary_domains_html usando sec_domain_html_id como clave.
        """
        self.logger.debug(
            f"[secondary_domains_html_sync] Upsert prod para sec_domain_html_id={row.get('sec_domain_html_id')}"
        )

        query = """
            INSERT INTO public.secondary_domains_html (
                sec_domain_html_id,
                html_content,
                sec_domain_id
            ) VALUES (
                %(sec_domain_html_id)s,
                %(html_content)s,
                %(sec_domain_id)s
            )
            ON CONFLICT (sec_domain_html_id) DO UPDATE SET
                html_content = EXCLUDED.html_content,
                sec_domain_id = EXCLUDED.sec_domain_id;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, sec_domain_html_id: int):
        """
        Marca processed=true en scraping.secondary_domains_html.
        """
        self.logger.debug(
            f"[secondary_domains_html_sync] Marcando processed=true para sec_domain_html_id={sec_domain_html_id}"
        )

        query = """
            UPDATE public.secondary_domains_html
            SET processed    = true,
                processed_at = NOW()
            WHERE sec_domain_html_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (sec_domain_html_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)
        if not rows:
            self.logger.info(
                "[secondary_domains_html_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[secondary_domains_html_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["sec_domain_html_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.logger.info(
                f"[secondary_domains_html_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[secondary_domains_html_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL secondary_domains_html_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL secondary_domains_html_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL secondary_domains_html_sync =====")

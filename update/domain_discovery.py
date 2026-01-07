# domain_discovery_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomainDiscoverySync:
    """
    Sincroniza public.domain_discovery desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.domain_discovery.
      - UPSERT en prod.domain_discovery usando ON CONFLICT(disc_domain_id).
      - Marca processed = true, processed_at = NOW() en scraping.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("domain_discovery_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (domain_discovery_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (domain_discovery_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (domain_discovery_sync)")
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
        Filas pendientes en scraping.domain_discovery (processed = false).
        """
        self.logger.info(
            f"[domain_discovery_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                disc_domain_id,
                online_status,
                status_details,
                status_msg
            FROM public.domain_discovery
            WHERE processed = false
            ORDER BY disc_domain_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[domain_discovery_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.domain_discovery usando disc_domain_id como clave.
        """
        self.logger.debug(
            f"[domain_discovery_sync] Upsert prod para disc_domain_id={row.get('disc_domain_id')}"
        )

        query = """
            INSERT INTO public.domain_discovery (
                disc_domain_id,
                online_status,
                status_details,
                status_msg
            ) VALUES (
                %(disc_domain_id)s,
                %(online_status)s,
                %(status_details)s,
                %(status_msg)s
            )
            ON CONFLICT (disc_domain_id) DO UPDATE SET
                online_status  = EXCLUDED.online_status,
                status_details = EXCLUDED.status_details,
                status_msg     = EXCLUDED.status_msg;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, disc_domain_id: int):
        """
        Marca processed=true en scraping.domain_discovery.
        """
        self.logger.debug(
            f"[domain_discovery_sync] Marcando processed=true para disc_domain_id={disc_domain_id}"
        )

        query = """
            UPDATE public.domain_discovery
            SET processed    = true,
                processed_at = NOW()
            WHERE disc_domain_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (disc_domain_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)
        if not rows:
            self.logger.info(
                "[domain_discovery_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[domain_discovery_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["disc_domain_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.logger.info(
                f"[domain_discovery_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[domain_discovery_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL domain_discovery_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL domain_discovery_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL domain_discovery_sync =====")

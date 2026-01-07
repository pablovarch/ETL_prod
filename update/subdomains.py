# subdomains_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class SubdomainsSync:
    """
    Sincroniza public.subdomains desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.subdomains.
      - Hace UPSERT en prod.subdomains usando ON CONFLICT(subdomain_id).
      - Marca processed = true, processed_at = NOW() en scraping.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("subdomains_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (subdomains_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (subdomains_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (subdomains_sync)")
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
        Filas pendientes en scraping.subdomains (processed = false).
        """
        self.logger.info(
            f"[subdomains_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                subdomain_id,
                subdomain,
                "domain",
                full_url,
                country,
                date_sourced,
                online_status,
                offline_type,
                domain_id
            FROM public.subdomains
            WHERE processed = false
            ORDER BY subdomain_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[subdomains_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.subdomains usando subdomain_id como clave.
        """
        self.logger.debug(
            f"[subdomains_sync] Upsert prod para subdomain_id={row.get('subdomain_id')}"
        )

        query = """
            INSERT INTO public.subdomains (
                subdomain_id,
                subdomain,
                "domain",
                full_url,
                country,
                date_sourced,
                online_status,
                offline_type,
                domain_id
            ) VALUES (
                %(subdomain_id)s,
                %(subdomain)s,
                %(domain)s,
                %(full_url)s,
                %(country)s,
                %(date_sourced)s,
                %(online_status)s,
                %(offline_type)s,
                %(domain_id)s
            )
            ON CONFLICT (subdomain_id) DO UPDATE SET
                subdomain     = EXCLUDED.subdomain,
                "domain"      = EXCLUDED."domain",
                full_url      = EXCLUDED.full_url,
                country       = EXCLUDED.country,
                date_sourced  = EXCLUDED.date_sourced,
                online_status = EXCLUDED.online_status,
                offline_type  = EXCLUDED.offline_type,
                domain_id     = EXCLUDED.domain_id;
        """

        params = {
            "subdomain_id": row["subdomain_id"],
            "subdomain": row["subdomain"],
            "domain": row["domain"],
            "full_url": row["full_url"],
            "country": row["country"],
            "date_sourced": row["date_sourced"],
            "online_status": row["online_status"],
            "offline_type": row["offline_type"],
            "domain_id": row["domain_id"],
        }

        with self.prod_conn.cursor() as cur:
            cur.execute(query, params)

    def mark_as_processed(self, subdomain_id: str):
        """
        Marca processed=true en scraping.subdomains.
        """
        self.logger.debug(
            f"[subdomains_sync] Marcando processed=true para subdomain_id={subdomain_id}"
        )

        query = """
            UPDATE public.subdomains
            SET processed    = true,
                processed_at = NOW()
            WHERE subdomain_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (subdomain_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)
        if not rows:
            self.logger.info(
                "[subdomains_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[subdomains_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["subdomain_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.logger.info(
                f"[subdomains_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[subdomains_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL subdomains_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL subdomains_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL subdomains_sync =====")

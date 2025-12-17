# ad_events.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class AdEvents:
    """
    ETL para sincronizar la tabla public.ad_events
    desde la base de scraping hacia la base de producción.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("ad_events_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos (ad_events)")

        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones establecidas correctamente (ad_events)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos (ad_events)")
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
        Obtiene filas pendientes (processed = false) desde la base de scraping.
        """
        self.logger.info(f"[ad_events] Buscando filas pendientes (limite={limit})")

        query = """
            SELECT
                ad_event_id,
                event_date,
                browser_profile_id,
                url,
                sec_domain,
                is_popup,
                domain_id,
                country,
                session_code,
                popup_landing_page,
                subdomain,
                online_status,
                offline_type,
                sec_domain_id,
                session_id
            FROM public.ad_events
            WHERE processed = false
            ORDER BY ad_event_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(f"[ad_events] Filas pendientes encontradas: {len(rows)}")
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en la tabla de producción.
        Usa ad_event_id como PK compartida.
        """
        self.logger.debug(
            f"[ad_events] Upsert en prod para ad_event_id={row.get('ad_event_id')}"
        )

        query = """
            INSERT INTO public.ad_events (
                ad_event_id,
                event_date,
                browser_profile_id,
                url,
                sec_domain,
                is_popup,
                domain_id,
                country,
                session_code,
                popup_landing_page,
                subdomain,
                online_status,
                offline_type,
                sec_domain_id,
                session_id
            ) VALUES (
                %(ad_event_id)s,
                %(event_date)s,
                %(browser_profile_id)s,
                %(url)s,
                %(sec_domain)s,
                %(is_popup)s,
                %(domain_id)s,
                %(country)s,
                %(session_code)s,
                %(popup_landing_page)s,
                %(subdomain)s,
                %(online_status)s,
                %(offline_type)s,
                %(sec_domain_id)s,
                %(session_id)s
            )
            ON CONFLICT (ad_event_id) DO UPDATE SET
                event_date         = EXCLUDED.event_date,
                browser_profile_id = EXCLUDED.browser_profile_id,
                url                = EXCLUDED.url,
                sec_domain         = EXCLUDED.sec_domain,
                is_popup           = EXCLUDED.is_popup,
                domain_id          = EXCLUDED.domain_id,
                country            = EXCLUDED.country,
                session_code       = EXCLUDED.session_code,
                popup_landing_page = EXCLUDED.popup_landing_page,
                subdomain          = EXCLUDED.subdomain,
                online_status      = EXCLUDED.online_status,
                offline_type       = EXCLUDED.offline_type,
                sec_domain_id      = EXCLUDED.sec_domain_id,
                session_id         = EXCLUDED.session_id
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, ad_event_id: int):
        """
        Marca una fila como procesada en la base de scraping.
        """
        self.logger.debug(
            f"[ad_events] Marcando como procesado ad_event_id={ad_event_id}"
        )

        query = """
            UPDATE public.ad_events
            SET processed = true,
                processed_at = NOW()
            WHERE ad_event_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (ad_event_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        """
        Procesa un batch:
        - Lee filas pendientes.
        - Hace upsert en prod.
        - Marca como processed en scraping.

        Devuelve cuántas filas procesó.
        """
        rows = self.fetch_pending_rows(self.batch_size)

        if not rows:
            self.logger.info("[ad_events] No hay filas pendientes para procesar")
            return 0

        self.logger.info(f"[ad_events] Procesando batch de {len(rows)} filas")

        try:
            for row in rows:
                self.logger.debug(
                    f"[ad_events] Procesando ad_event_id={row['ad_event_id']} "
                    f"(url={row['url']}, session_code={row['session_code']})"
                )
                self.upsert_into_prod(row)
                self.mark_as_processed(row["ad_event_id"])

            # Commit del batch en ambas bases
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[ad_events] Batch procesado correctamente ({len(rows)} filas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[ad_events] Error procesando batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()
            # opcional: raise si querés que el proceso externo falle
            # raise

        return len(rows)

    # ---------- Punto de entrada de la clase ----------

    def run(self):
        """
        Ejecuta el ETL completo:
        - Abre conexiones
        - Procesa batches hasta quedarse sin pendientes
        - Cierra conexiones
        """
        self.logger.info("===== Inicio ETL ad_events =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL ad_events finalizado. Total de filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL ad_events =====")

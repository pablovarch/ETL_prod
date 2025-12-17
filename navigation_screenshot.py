# navigation_screenshots.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class NavigationScreenshots:
    """
    ETL para sincronizar la tabla public.navigation_screenshots
    desde la base de scraping hacia la base de producción.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("navigation_screenshots_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos (navigation_screenshots)")

        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones establecidas correctamente (navigation_screenshots)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos (navigation_screenshots)")
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
        Se filtra también nav_screenshot_id IS NOT NULL por seguridad.
        """
        self.logger.info(f"[navigation_screenshots] Buscando filas pendientes (limite={limit})")

        query = """
            SELECT
                nav_screenshot_id,
                screenshot_name,
                screenshot_file,
                ad_event_id
            FROM public.navigation_screenshots
            WHERE processed = false
              AND nav_screenshot_id IS NOT NULL
            ORDER BY nav_screenshot_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[navigation_screenshots] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en la tabla de producción.
        Usa nav_screenshot_id como PK compartida.
        """
        self.logger.debug(
            "[navigation_screenshots] Upsert en prod para "
            f"nav_screenshot_id={row.get('nav_screenshot_id')}"
        )

        query = """
            INSERT INTO public.navigation_screenshots (
                nav_screenshot_id,
                screenshot_name,
                screenshot_file,
                ad_event_id
            ) VALUES (
                %(nav_screenshot_id)s,
                %(screenshot_name)s,
                %(screenshot_file)s,
                %(ad_event_id)s
            )
            ON CONFLICT (nav_screenshot_id) DO UPDATE SET
                screenshot_name = EXCLUDED.screenshot_name,
                screenshot_file = EXCLUDED.screenshot_file,
                ad_event_id     = EXCLUDED.ad_event_id
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, nav_screenshot_id: int):
        """
        Marca una fila como procesada en la base de scraping.
        """
        self.logger.debug(
            "[navigation_screenshots] Marcando como procesado "
            f"nav_screenshot_id={nav_screenshot_id}"
        )

        query = """
            UPDATE public.navigation_screenshots
            SET processed = true,
                processed_at = NOW()
            WHERE nav_screenshot_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (nav_screenshot_id,))

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
            self.logger.info(
                "[navigation_screenshots] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[navigation_screenshots] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.logger.debug(
                    "[navigation_screenshots] Procesando "
                    f"nav_screenshot_id={row['nav_screenshot_id']} "
                    f"(ad_event_id={row['ad_event_id']})"
                )
                self.upsert_into_prod(row)
                self.mark_as_processed(row["nav_screenshot_id"])

            # Commit del batch en ambas bases
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                "[navigation_screenshots] Batch procesado correctamente "
                f"({len(rows)} filas)"
            )
        except Exception as e:
            self.logger.exception(
                "[navigation_screenshots] Error procesando batch, "
                f"rollback en ambas bases: {e}"
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
        self.logger.info("===== Inicio ETL navigation_screenshots =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                "ETL navigation_screenshots finalizado. "
                f"Total de filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL navigation_screenshots =====")

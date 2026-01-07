# dom_content.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomContent:
    """
    ETL para sincronizar la tabla public.dom_content
    desde la base de scraping hacia la base de producción.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("dom_content_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos (dom_content)")

        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones establecidas correctamente (dom_content)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos (dom_content)")
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
        self.logger.info(f"[dom_content] Buscando filas pendientes (limite={limit})")

        query = """
            SELECT
                dom_content_id,
                dom_content_label,
                dom_content,
                ad_event_id
            FROM public.dom_content
            WHERE processed = false
            ORDER BY dom_content_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(f"[dom_content] Filas pendientes encontradas: {len(rows)}")
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en la tabla de producción.
        Usa dom_content_id como PK compartida.
        """
        self.logger.debug(
            f"[dom_content] Upsert en prod para dom_content_id={row.get('dom_content_id')}"
        )

        query = """
            INSERT INTO public.dom_content (
                dom_content_id,
                dom_content_label,
                dom_content,
                ad_event_id
            ) VALUES (
                %(dom_content_id)s,
                %(dom_content_label)s,
                %(dom_content)s,
                %(ad_event_id)s
            )
            ON CONFLICT (dom_content_id) DO UPDATE SET
                dom_content_label = EXCLUDED.dom_content_label,
                dom_content       = EXCLUDED.dom_content,
                ad_event_id       = EXCLUDED.ad_event_id
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, dom_content_id: int):
        """
        Marca una fila como procesada en la base de scraping.
        """
        self.logger.debug(
            f"[dom_content] Marcando como procesado dom_content_id={dom_content_id}"
        )

        query = """
            UPDATE public.dom_content
            SET processed = true,
                processed_at = NOW()
            WHERE dom_content_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (dom_content_id,))

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
            self.logger.info("[dom_content] No hay filas pendientes para procesar")
            return 0

        self.logger.info(f"[dom_content] Procesando batch de {len(rows)} filas")

        try:
            for row in rows:
                self.logger.debug(
                    f"[dom_content] Procesando dom_content_id={row['dom_content_id']} "
                    f"(ad_event_id={row['ad_event_id']})"
                )
                self.upsert_into_prod(row)
                self.mark_as_processed(row["dom_content_id"])

            # Commit del batch en ambas bases
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[dom_content] Batch procesado correctamente ({len(rows)} filas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[dom_content] Error procesando batch, rollback en ambas bases: {e}"
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
        self.logger.info("===== Inicio ETL dom_content =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL dom_content finalizado. Total de filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL dom_content =====")

# browser_urls_seq.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class BrowserUrlsSeq:
    """
    ETL para sincronizar la tabla public.browser_urls_seq
    desde la base de scraping hacia la base de producción.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("browser_urls_seq_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos (browser_urls_seq)")

        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones establecidas correctamente (browser_urls_seq)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos (browser_urls_seq)")
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
        self.logger.info(f"[browser_urls_seq] Buscando filas pendientes (limite={limit})")

        query = """
            SELECT
                browser_urls_seq_id,
                ad_event_id,
                "domain" AS domain,
                url,
                sequence_num
            FROM public.browser_urls_seq
            WHERE processed = false
            ORDER BY browser_urls_seq_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(f"[browser_urls_seq] Filas pendientes encontradas: {len(rows)}")
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en la tabla de producción.
        Usa browser_urls_seq_id como PK compartida.
        """
        self.logger.debug(
            "[browser_urls_seq] Upsert en prod para "
            f"browser_urls_seq_id={row.get('browser_urls_seq_id')}"
        )

        query = """
            INSERT INTO public.browser_urls_seq (
                browser_urls_seq_id,
                ad_event_id,
                "domain",
                url,
                sequence_num
            ) VALUES (
                %(browser_urls_seq_id)s,
                %(ad_event_id)s,
                %(domain)s,
                %(url)s,
                %(sequence_num)s
            )
            ON CONFLICT (browser_urls_seq_id) DO UPDATE SET
                ad_event_id  = EXCLUDED.ad_event_id,
                "domain"     = EXCLUDED."domain",
                url          = EXCLUDED.url,
                sequence_num = EXCLUDED.sequence_num
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, browser_urls_seq_id: int):
        """
        Marca una fila como procesada en la base de scraping.
        """
        self.logger.debug(
            "[browser_urls_seq] Marcando como procesado "
            f"browser_urls_seq_id={browser_urls_seq_id}"
        )

        query = """
            UPDATE public.browser_urls_seq
            SET processed = true,
                processed_at = NOW()
            WHERE browser_urls_seq_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (browser_urls_seq_id,))

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
            self.logger.info("[browser_urls_seq] No hay filas pendientes para procesar")
            return 0

        self.logger.info(f"[browser_urls_seq] Procesando batch de {len(rows)} filas")

        try:
            for row in rows:
                self.logger.debug(
                    "[browser_urls_seq] Procesando "
                    f"browser_urls_seq_id={row['browser_urls_seq_id']} "
                    f"(ad_event_id={row['ad_event_id']}, domain={row['domain']})"
                )
                self.upsert_into_prod(row)
                self.mark_as_processed(row["browser_urls_seq_id"])

            # Commit del batch en ambas bases
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[browser_urls_seq] Batch procesado correctamente ({len(rows)} filas)"
            )
        except Exception as e:
            self.logger.exception(
                "[browser_urls_seq] Error procesando batch, "
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
        self.logger.info("===== Inicio ETL browser_urls_seq =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                "ETL browser_urls_seq finalizado. "
                f"Total de filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL browser_urls_seq =====")

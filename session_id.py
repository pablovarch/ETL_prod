# session_id_gen.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class SessionIdGen:
    """
    ETL para sincronizar la tabla public.session_id_gen
    desde la base de scraping hacia la base de producción.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("session_id_gen_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos (session_id_gen)")

        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones establecidas correctamente (session_id_gen)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos (session_id_gen)")
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
        self.logger.info(f"[session_id_gen] Buscando filas pendientes (limite={limit})")

        query = """
            SELECT
                session_id,
                source_domain,
                "timestamp" AS timestamp
            FROM public.session_id_gen
            WHERE processed = false
            ORDER BY session_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(f"[session_id_gen] Filas pendientes encontradas: {len(rows)}")
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en la tabla de producción.
        Usa session_id como PK compartida.
        """
        self.logger.debug(
            f"[session_id_gen] Upsert en prod para session_id={row.get('session_id')}"
        )

        query = """
            INSERT INTO public.session_id_gen (
                session_id,
                source_domain,
                "timestamp"
            ) VALUES (
                %(session_id)s,
                %(source_domain)s,
                %(timestamp)s
            )
            ON CONFLICT (session_id) DO UPDATE SET
                source_domain = EXCLUDED.source_domain,
                "timestamp"   = EXCLUDED."timestamp"
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, session_id: int):
        """
        Marca una fila como procesada en la base de scraping.
        """
        self.logger.debug(
            f"[session_id_gen] Marcando como procesado session_id={session_id}"
        )

        query = """
            UPDATE public.session_id_gen
            SET processed = true,
                processed_at = NOW()
            WHERE session_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (session_id,))

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
            self.logger.info("[session_id_gen] No hay filas pendientes para procesar")
            return 0

        self.logger.info(f"[session_id_gen] Procesando batch de {len(rows)} filas")

        try:
            for row in rows:
                self.logger.debug(
                    f"[session_id_gen] Procesando session_id={row['session_id']} "
                    f"(source_domain={row['source_domain']})"
                )
                self.upsert_into_prod(row)
                self.mark_as_processed(row["session_id"])

            # Commit del batch en ambas bases
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[session_id_gen] Batch procesado correctamente ({len(rows)} filas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[session_id_gen] Error procesando batch, rollback en ambas bases: {e}"
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
        self.logger.info("===== Inicio ETL session_id_gen =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL session_id_gen finalizado. Total de filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL session_id_gen =====")

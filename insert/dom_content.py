# dom_content_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomContentSync:
    """
    Sincroniza public.dom_content desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.dom_content.
      - En prod:
          * UPDATE por dom_content_id
          * Si no actualiza ninguna fila, hace INSERT
      - Marca processed = true, processed_at = NOW() en scraping.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("dom_content_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (dom_content_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (dom_content_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (dom_content_sync)")
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
        Filas pendientes en scraping.dom_content (processed = false).
        """
        self.logger.info(
            f"[dom_content_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                dom_content_id,
                dom_content_label,
                dom_content,
                ad_event_id,
                privacy_policy,
                terms_of_use
            FROM public.dom_content
            WHERE processed = false
            ORDER BY dom_content_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[dom_content_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en prod.dom_content SIN usar ON CONFLICT.
        - Primero intenta UPDATE por dom_content_id.
        - Si no afecta filas, hace INSERT.
        """
        dom_content_id = row.get("dom_content_id")
        self.logger.debug(
            f"[dom_content_sync] Upsert (UPDATE/INSERT) en prod para dom_content_id={dom_content_id}"
        )

        update_query = """
            UPDATE public.dom_content
            SET
                dom_content_label = %s,
                dom_content       = %s,
                ad_event_id       = %s,
                privacy_policy    = %s,
                terms_of_use      = %s
            WHERE dom_content_id = %s
        """

        update_params = (
            row["dom_content_label"],
            row["dom_content"],
            row["ad_event_id"],
            row["privacy_policy"],
            row["terms_of_use"],
            dom_content_id,
        )

        with self.prod_conn.cursor() as cur:
            # Intentar UPDATE
            cur.execute(update_query, update_params)

            if cur.rowcount == 0:
                # No existía → INSERT
                insert_query = """
                    INSERT INTO public.dom_content (
                        dom_content_id,
                        dom_content_label,
                        dom_content,
                        ad_event_id,
                        privacy_policy,
                        terms_of_use
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                insert_params = (
                    dom_content_id,
                    row["dom_content_label"],
                    row["dom_content"],
                    row["ad_event_id"],
                    row["privacy_policy"],
                    row["terms_of_use"],
                )

                self.logger.debug(
                    f"[dom_content_sync] INSERT en prod para dom_content_id={dom_content_id}"
                )
                cur.execute(insert_query, insert_params)
            else:
                self.logger.debug(
                    f"[dom_content_sync] UPDATE aplicado en prod para dom_content_id={dom_content_id}"
                )

    def mark_as_processed(self, dom_content_id: int):
        """
        Marca processed=true en scraping.dom_content.
        """
        self.logger.debug(
            f"[dom_content_sync] Marcando processed=true para dom_content_id={dom_content_id}"
        )

        query = """
            UPDATE public.dom_content
            SET processed    = true,
                processed_at = NOW()
            WHERE dom_content_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (dom_content_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)
        if not rows:
            self.logger.info(
                "[dom_content_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[dom_content_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["dom_content_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.logger.info(
                f"[dom_content_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[dom_content_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL dom_content_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL dom_content_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL dom_content_sync =====")

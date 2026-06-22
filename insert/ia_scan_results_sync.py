import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class IaScanResultsSync:
    """
    Sincroniza public.ia_scan_results desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.ia_scan_results.
      - Hace UPSERT en prod.ia_scan_results usando ia_scan_result_id.
      - Marca processed = true, processed_at = NOW() en scraping.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("ia_scan_results_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (ia_scan_results_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (ia_scan_results_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (ia_scan_results_sync)")
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
        Lee filas pendientes desde scraping.ia_scan_results.
        """
        self.logger.info(
            f"[ia_scan_results_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                ia_scan_result_id,
                domain_id,
                ad_event_id,
                created_at,
                success,
                steps_taken,
                final_url,
                failure_mode,
                media_type,
                is_target_site,
                interstitial_found,
                interstitial_bypassed,
                content_reached,
                content_url,
                player_reached,
                navigation_result,
                navigation_trace,
                detail_reached
            FROM public.ia_scan_results
            WHERE processed = false
            ORDER BY ia_scan_result_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[ia_scan_results_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en prod usando ia_scan_result_id.
        """
        self.logger.debug(
            f"[ia_scan_results_sync] Upsert prod para ia_scan_result_id={row.get('ia_scan_result_id')}"
        )

        query = """
            INSERT INTO public.ia_scan_results (
                ia_scan_result_id,
                domain_id,
                ad_event_id,
                created_at,
                success,
                steps_taken,
                final_url,
                failure_mode,
                media_type,
                is_target_site,
                interstitial_found,
                interstitial_bypassed,
                content_reached,
                content_url,
                player_reached,
                navigation_result,
                navigation_trace,
                detail_reached
            ) VALUES (
                %(ia_scan_result_id)s,
                %(domain_id)s,
                %(ad_event_id)s,
                %(created_at)s,
                %(success)s,
                %(steps_taken)s,
                %(final_url)s,
                %(failure_mode)s,
                %(media_type)s,
                %(is_target_site)s,
                %(interstitial_found)s,
                %(interstitial_bypassed)s,
                %(content_reached)s,
                %(content_url)s,
                %(player_reached)s,
                %(navigation_result)s,
                %(navigation_trace)s,
                %(detail_reached)s
            )
            ON CONFLICT (ia_scan_result_id) DO UPDATE SET
                domain_id              = EXCLUDED.domain_id,
                ad_event_id            = EXCLUDED.ad_event_id,
                created_at             = EXCLUDED.created_at,
                success                = EXCLUDED.success,
                steps_taken            = EXCLUDED.steps_taken,
                final_url              = EXCLUDED.final_url,
                failure_mode           = EXCLUDED.failure_mode,
                media_type             = EXCLUDED.media_type,
                is_target_site         = EXCLUDED.is_target_site,
                interstitial_found     = EXCLUDED.interstitial_found,
                interstitial_bypassed  = EXCLUDED.interstitial_bypassed,
                content_reached        = EXCLUDED.content_reached,
                content_url            = EXCLUDED.content_url,
                player_reached         = EXCLUDED.player_reached,
                navigation_result      = EXCLUDED.navigation_result,
                navigation_trace       = EXCLUDED.navigation_trace,
                detail_reached         = EXCLUDED.detail_reached
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, ia_scan_result_id: int):
        """
        Marca la fila como procesada en scraping.
        """
        self.logger.debug(
            f"[ia_scan_results_sync] Marcando processed=true para ia_scan_result_id={ia_scan_result_id}"
        )

        query = """
            UPDATE public.ia_scan_results
            SET processed = true,
                processed_at = NOW()
            WHERE ia_scan_result_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (ia_scan_result_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)

        if not rows:
            self.logger.info(
                "[ia_scan_results_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[ia_scan_results_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["ia_scan_result_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[ia_scan_results_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[ia_scan_results_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL ia_scan_results_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL ia_scan_results_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL ia_scan_results_sync =====")
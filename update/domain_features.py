# domain_features_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomainFeaturesSync:
    """
    Sincroniza public.domain_features desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.domain_features.
      - En prod:
          * UPDATE por dfeatures_id
          * Si no actualiza ninguna fila, hace INSERT
      - Marca processed = true, processed_at = NOW() en scraping (por dfeatures_id).
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("domain_features_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (domain_features_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (domain_features_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (domain_features_sync)")
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
        Filas pendientes en scraping.domain_features (processed = false).
        """
        self.logger.info(
            f"[domain_features_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                dfeatures_id,
                num_popups,
                domain_id,
                html_text,
                homepage_button,
                last_update,
                exc_domain_id,
                terms_of_use_url,
                terms_of_use,
                dmca,
                privacy_policy,
                domain_name,
                "source",
                disc_domain_id
            FROM public.domain_features
            WHERE processed = false
            ORDER BY dfeatures_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[domain_features_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en prod.domain_features SIN usar ON CONFLICT.
        Lógica:
          - Primero intenta UPDATE por domain_name (clave lógica / UNIQUE).
          - Si no afecta filas, hace INSERT.
        """
        dfeatures_id = row.get("dfeatures_id")
        domain_name = row.get("domain_name")

        self.logger.debug(
            f"[domain_features_sync] Upsert (UPDATE/INSERT) en prod para "
            f"dfeatures_id={dfeatures_id}, domain_name={domain_name}"
        )

        # 1) Intentar UPDATE por domain_name (constraint UNIQUE)
        update_query = """
            UPDATE public.domain_features
            SET
                dfeatures_id      = %s,
                num_popups        = %s,
                domain_id         = %s,
                html_text         = %s,
                homepage_button   = %s,
                last_update       = %s,
                exc_domain_id     = %s,
                terms_of_use_url  = %s,
                terms_of_use      = %s,
                dmca              = %s,
                privacy_policy    = %s,
                "source"          = %s,
                disc_domain_id    = %s
            WHERE domain_name = %s
        """

        update_params = (
            dfeatures_id,
            row["num_popups"],
            row["domain_id"],
            row["html_text"],
            row["homepage_button"],
            row["last_update"],
            row["exc_domain_id"],
            row["terms_of_use_url"],
            row["terms_of_use"],
            row["dmca"],
            row["privacy_policy"],
            row["source"],
            row["disc_domain_id"],
            domain_name,
        )

        with self.prod_conn.cursor() as cur:
            cur.execute(update_query, update_params)

            if cur.rowcount == 0:
                # 2) No existía ese domain_name en prod → INSERT
                insert_query = """
                    INSERT INTO public.domain_features (
                        dfeatures_id,
                        num_popups,
                        domain_id,
                        html_text,
                        homepage_button,
                        last_update,
                        exc_domain_id,
                        terms_of_use_url,
                        terms_of_use,
                        dmca,
                        privacy_policy,
                        domain_name,
                        "source",
                        disc_domain_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                insert_params = (
                    dfeatures_id,
                    row["num_popups"],
                    row["domain_id"],
                    row["html_text"],
                    row["homepage_button"],
                    row["last_update"],
                    row["exc_domain_id"],
                    row["terms_of_use_url"],
                    row["terms_of_use"],
                    row["dmca"],
                    row["privacy_policy"],
                    domain_name,
                    row["source"],
                    row["disc_domain_id"],
                )

                self.logger.debug(
                    f"[domain_features_sync] INSERT en prod para domain_name={domain_name}"
                )
                cur.execute(insert_query, insert_params)
            else:
                self.logger.debug(
                    f"[domain_features_sync] UPDATE aplicado en prod para domain_name={domain_name}"
                )

    def mark_as_processed(self, dfeatures_id: int):
        """
        Marca processed=true en scraping.domain_features (por dfeatures_id).
        """
        self.logger.debug(
            f"[domain_features_sync] Marcando processed=true para dfeatures_id={dfeatures_id}"
        )

        query = """
            UPDATE public.domain_features
            SET processed    = true,
                processed_at = NOW()
            WHERE dfeatures_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (dfeatures_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)
        if not rows:
            self.logger.info(
                "[domain_features_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[domain_features_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["dfeatures_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.logger.info(
                f"[domain_features_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[domain_features_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL domain_features_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL domain_features_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL domain_features_sync =====")

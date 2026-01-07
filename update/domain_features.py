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
      - Hace UPSERT en prod.domain_features usando ON CONFLICT(domain_id).
      - Marca processed = true, processed_at = NOW() en scraping.
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
                contact,
                domain_name,
                "source",
                disc_domain_id
            FROM public.domain_features
            WHERE processed = false
              AND domain_id IS NOT NULL
            ORDER BY domain_id
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
        UPSERT en prod.domain_features usando domain_id como clave.
        """
        self.logger.debug(
            f"[domain_features_sync] Upsert prod para domain_id={row.get('domain_id')}"
        )

        query = """
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
                contact,
                domain_name,
                "source",
                disc_domain_id
            ) VALUES (
                %(dfeatures_id)s,
                %(num_popups)s,
                %(domain_id)s,
                %(html_text)s,
                %(homepage_button)s,
                %(last_update)s,
                %(exc_domain_id)s,
                %(terms_of_use_url)s,
                %(terms_of_use)s,
                %(dmca)s,
                %(privacy_policy)s,
                %(contact)s,
                %(domain_name)s,
                %(source)s,
                %(disc_domain_id)s
            )
            ON CONFLICT (domain_id) DO UPDATE SET
                dfeatures_id     = EXCLUDED.dfeatures_id,
                num_popups       = EXCLUDED.num_popups,
                html_text        = EXCLUDED.html_text,
                homepage_button  = EXCLUDED.homepage_button,
                last_update      = EXCLUDED.last_update,
                exc_domain_id    = EXCLUDED.exc_domain_id,
                terms_of_use_url = EXCLUDED.terms_of_use_url,
                terms_of_use     = EXCLUDED.terms_of_use,
                dmca             = EXCLUDED.dmca,
                privacy_policy   = EXCLUDED.privacy_policy,
                contact          = EXCLUDED.contact,
                domain_name      = EXCLUDED.domain_name,
                "source"         = EXCLUDED."source",
                disc_domain_id   = EXCLUDED.disc_domain_id;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, domain_id: int):
        """
        Marca processed=true en scraping.domain_features.
        """
        self.logger.debug(
            f"[domain_features_sync] Marcando processed=true para domain_id={domain_id}"
        )

        query = """
            UPDATE public.domain_features
            SET processed    = true,
                processed_at = NOW()
            WHERE domain_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (domain_id,))

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
                self.mark_as_processed(row["domain_id"])

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

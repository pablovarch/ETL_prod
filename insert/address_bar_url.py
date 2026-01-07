# address_bar_url.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class AddressBarUrl:
    """
    ETL para sincronizar la tabla public.address_bar_url
    desde la base de scraping hacia la base de producción.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("address_bar_url_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos (address_bar_url)")

        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones establecidas correctamente (address_bar_url)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos (address_bar_url)")
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
        self.logger.info(f"[address_bar_url] Buscando filas pendientes (limite={limit})")

        query = """
            SELECT
                address_bar_id,
                address_bar_num,
                address_bar_domain,
                ad_event_id,
                source_domain,
                collection_date,
                referral_cloaking_domain,
                redirect,
                address_bar_url,
                landing_page,
                session_id,
                tab_num
            FROM public.address_bar_url
            WHERE processed = false
            ORDER BY address_bar_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(f"[address_bar_url] Filas pendientes encontradas: {len(rows)}")
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en la tabla de producción.
        Usa address_bar_id como PK compartida.
        """
        self.logger.debug(
            f"[address_bar_url] Upsert en prod para address_bar_id={row.get('address_bar_id')}"
        )

        query = """
            INSERT INTO public.address_bar_url (
                address_bar_id,
                address_bar_num,
                address_bar_domain,
                ad_event_id,
                source_domain,
                collection_date,
                referral_cloaking_domain,
                redirect,
                address_bar_url,
                landing_page,
                session_id,
                tab_num
            ) VALUES (
                %(address_bar_id)s,
                %(address_bar_num)s,
                %(address_bar_domain)s,
                %(ad_event_id)s,
                %(source_domain)s,
                %(collection_date)s,
                %(referral_cloaking_domain)s,
                %(redirect)s,
                %(address_bar_url)s,
                %(landing_page)s,
                %(session_id)s,
                %(tab_num)s
            )
            ON CONFLICT (address_bar_id) DO UPDATE SET
                address_bar_num          = EXCLUDED.address_bar_num,
                address_bar_domain       = EXCLUDED.address_bar_domain,
                ad_event_id              = EXCLUDED.ad_event_id,
                source_domain            = EXCLUDED.source_domain,
                collection_date          = EXCLUDED.collection_date,
                referral_cloaking_domain = EXCLUDED.referral_cloaking_domain,
                redirect                 = EXCLUDED.redirect,
                address_bar_url          = EXCLUDED.address_bar_url,
                landing_page             = EXCLUDED.landing_page,
                session_id               = EXCLUDED.session_id,
                tab_num                  = EXCLUDED.tab_num
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, address_bar_id: int):
        """
        Marca una fila como procesada en la base de scraping.
        """
        self.logger.debug(
            f"[address_bar_url] Marcando como procesado address_bar_id={address_bar_id}"
        )

        query = """
            UPDATE public.address_bar_url
            SET processed = true,
                processed_at = NOW()
            WHERE address_bar_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (address_bar_id,))

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
            self.logger.info("[address_bar_url] No hay filas pendientes para procesar")
            return 0

        self.logger.info(f"[address_bar_url] Procesando batch de {len(rows)} filas")

        try:
            for row in rows:
                self.logger.debug(
                    f"[address_bar_url] Procesando address_bar_id={row['address_bar_id']} "
                    f"(ad_event_id={row['ad_event_id']}, source_domain={row['source_domain']})"
                )
                self.upsert_into_prod(row)
                self.mark_as_processed(row["address_bar_id"])

            # Commit del batch en ambas bases
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[address_bar_url] Batch procesado correctamente ({len(rows)} filas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[address_bar_url] Error procesando batch, rollback en ambas bases: {e}"
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
        self.logger.info("===== Inicio ETL address_bar_url =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL address_bar_url finalizado. Total de filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL address_bar_url =====")

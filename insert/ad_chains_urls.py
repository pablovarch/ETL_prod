# ad_chains_urls.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class AdChainsUrls:
    """
    ETL para sincronizar la tabla public.ad_chain_urls
    desde la base de scraping hacia la base de producción.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("ad_chains_urls_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos")

        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones establecidas correctamente")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos")
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
        self.logger.info(f"Buscando filas pendientes en scraping (limite={limit})")

        query = """
            SELECT
                ad_url_id,
                ad_url_num,
                ad_url,
                ad_url_domain,
                ad_event_id,
                source_domain,
                ad_domain_id,
                collection_timestamp,
                collection_date,
                status_code,
                has_content,
                referral_cloaking_domain
            FROM public.ad_chain_urls
            WHERE processed = false
            ORDER BY ad_url_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(f"Filas pendientes encontradas: {len(rows)}")
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en la tabla de producción.
        Usa ad_url_id como PK compartida.
        """
        self.logger.debug(f"Upsert en prod para ad_url_id={row.get('ad_url_id')}")

        query = """
            INSERT INTO public.ad_chain_urls (
                ad_url_id,
                ad_url_num,
                ad_url,
                ad_url_domain,
                ad_event_id,
                source_domain,
                ad_domain_id,
                collection_timestamp,
                collection_date,
                status_code,
                has_content,
                referral_cloaking_domain
            ) VALUES (
                %(ad_url_id)s,
                %(ad_url_num)s,
                %(ad_url)s,
                %(ad_url_domain)s,
                %(ad_event_id)s,
                %(source_domain)s,
                %(ad_domain_id)s,
                %(collection_timestamp)s,
                %(collection_date)s,
                %(status_code)s,
                %(has_content)s,
                %(referral_cloaking_domain)s
            )
            ON CONFLICT (ad_url_id) DO UPDATE SET
                ad_url_num               = EXCLUDED.ad_url_num,
                ad_url                   = EXCLUDED.ad_url,
                ad_url_domain            = EXCLUDED.ad_url_domain,
                ad_event_id              = EXCLUDED.ad_event_id,
                source_domain            = EXCLUDED.source_domain,
                ad_domain_id             = EXCLUDED.ad_domain_id,
                collection_timestamp     = EXCLUDED.collection_timestamp,
                collection_date          = EXCLUDED.collection_date,
                status_code              = EXCLUDED.status_code,
                has_content              = EXCLUDED.has_content,
                referral_cloaking_domain = EXCLUDED.referral_cloaking_domain
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, ad_url_id: int):
        """
        Marca una fila como procesada en la base de scraping.
        """
        self.logger.debug(f"Marcando como procesado ad_url_id={ad_url_id}")

        query = """
            UPDATE public.ad_chain_urls
            SET processed = true,
                processed_at = NOW()
            WHERE ad_url_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (ad_url_id,))

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
            self.logger.info("No hay filas pendientes para procesar")
            return 0

        self.logger.info(f"Procesando batch de {len(rows)} filas")

        try:
            for row in rows:
                self.logger.debug(
                    f"Procesando ad_url_id={row['ad_url_id']} "
                    f"(source_domain={row['source_domain']}, ad_event_id={row['ad_event_id']})"
                )
                self.upsert_into_prod(row)
                self.mark_as_processed(row["ad_url_id"])

            # Commit del batch en ambas bases
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(f"Batch procesado correctamente ({len(rows)} filas)")
        except Exception as e:
            self.logger.exception(
                f"Error procesando batch, haciendo rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()
            # opcional: relanzar si querés que reviente el proceso externo
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
        self.logger.info("===== Inicio ETL ad_chain_urls =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL finalizado. Total de filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL ad_chain_urls =====")

import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomainDiscoveryFromProd:
    """
    Sincroniza public.domain_discovery desde prod_db hacia scraping_db.

    Lógica:
      - En PROD:
          * Lee solo registros con processed = false.
      - En SCRAPING:
          * Solo INSERT si no existe el disc_domain.
          * No hace UPDATE.
          * Inserta disc_domain_id usando el valor de producción.
      - En PROD:
          * Después de sincronizar, marca processed = true, processed_at = NOW()
            para ese disc_domain_id.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("domain_discovery_from_prod_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.prod_conn = None
        self.scraping_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (domain_discovery_from_prod)")
        try:
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.logger.info("Conexiones OK (domain_discovery_from_prod)")
        except Exception as e:
            self.logger.exception(
                f"Error al conectar a las bases (domain_discovery_from_prod): {e}"
            )
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (domain_discovery_from_prod)")
        try:
            if self.prod_conn:
                self.prod_conn.close()
            if self.scraping_conn:
                self.scraping_conn.close()
        except Exception as e:
            self.logger.exception(
                f"Error al cerrar conexiones (domain_discovery_from_prod): {e}"
            )

    # ---------- PROD: leer + marcar processed ----------

    def fetch_rows_from_prod(self) -> list[dict]:
        """
        Trae un batch de filas desde prod.domain_discovery con processed = false.
        """
        self.logger.info(
            f"[domain_discovery_from_prod] Leyendo desde prod (processed = false, limite={self.batch_size})"
        )

        query = """
            SELECT
                disc_domain_id,
                disc_domain,
                keyword,
                whois_id,
                first_add,
                new_domain,
                age_of_registration,
                estimated_domain_age,
                commercial_registrant,
                likely_commercial_registrant,
                registrar_name,
                ping,
                tracert,
                domain_id,
                ml_piracy,
                site_url,
                site_domain,
                status_msg,
                "source",
                tenant,
                exc_domain_id,
                disc_domain_root,
                is_iptv,
                subdomain_host,
                ml_media_type_id,
                ml_dd_classification_id
            FROM public.domain_discovery
            WHERE processed = false
            ORDER BY disc_domain_id
            LIMIT %s
        """

        with self.prod_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (self.batch_size,))
            rows = cur.fetchall()

        self.logger.info(
            f"[domain_discovery_from_prod] Filas leídas desde prod: {len(rows)}"
        )
        return rows

    def mark_prod_as_processed(self, disc_domain_id: int):
        """
        Marca processed = true en prod.domain_discovery para ese disc_domain_id.
        """
        self.logger.debug(
            f"[domain_discovery_from_prod] Marcando processed=true en PROD para disc_domain_id={disc_domain_id}"
        )

        query = """
            UPDATE public.domain_discovery
            SET processed    = true,
                processed_at = NOW()
            WHERE disc_domain_id = %s
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, (disc_domain_id,))

    # ---------- SCRAPING ----------

    def exists_in_scraping(self, disc_domain: str) -> bool:
        """
        Verifica si el disc_domain ya existe en scraping.
        """
        query = """
            SELECT 1
            FROM public.domain_discovery
            WHERE disc_domain = %s
            LIMIT 1
        """
        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (disc_domain,))
            return cur.fetchone() is not None

    def insert_into_scraping(self, row: dict):
        """
        Inserta una fila nueva en scraping copiando disc_domain_id desde prod.
        """
        disc_domain = row["disc_domain"]
        disc_domain_id = row["disc_domain_id"]

        self.logger.debug(
            f"[domain_discovery_from_prod] INSERT en scraping para disc_domain={disc_domain}, disc_domain_id={disc_domain_id}"
        )

        insert_query = """
            INSERT INTO public.domain_discovery (
                disc_domain_id,
                disc_domain,
                keyword,
                whois_id,
                first_add,
                new_domain,
                age_of_registration,
                estimated_domain_age,
                commercial_registrant,
                likely_commercial_registrant,
                registrar_name,
                ping,
                tracert,
                domain_id,
                ml_piracy,
                site_url,
                site_domain,
                status_msg,
                "source",
                tenant,
                exc_domain_id,
                disc_domain_root,
                is_iptv,
                subdomain_host,
                ml_media_type_id,
                ml_dd_classification_id
            ) VALUES (
                %(disc_domain_id)s,
                %(disc_domain)s,
                %(keyword)s,
                %(whois_id)s,
                %(first_add)s,
                %(new_domain)s,
                %(age_of_registration)s,
                %(estimated_domain_age)s,
                %(commercial_registrant)s,
                %(likely_commercial_registrant)s,
                %(registrar_name)s,
                %(ping)s,
                %(tracert)s,
                %(domain_id)s,
                %(ml_piracy)s,
                %(site_url)s,
                %(site_domain)s,
                %(status_msg)s,
                %(source)s,
                %(tenant)s,
                %(exc_domain_id)s,
                %(disc_domain_root)s,
                %(is_iptv)s,
                %(subdomain_host)s,
                %(ml_media_type_id)s,
                %(ml_dd_classification_id)s
            )
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(insert_query, row)

    # ---------- Orquestación ----------

    def process_all(self) -> int:
        """
        Recorre prod.domain_discovery en batches (processed = false)
        y sincroniza hacia scraping.
        """
        total_processed = 0

        while True:
            rows = self.fetch_rows_from_prod()
            if not rows:
                break

            batch_count = 0
            try:
                for row in rows:
                    disc_domain = row["disc_domain"]

                    if self.exists_in_scraping(disc_domain):
                        self.logger.info(
                            f"[domain_discovery_from_prod] disc_domain ya existe en scraping, no se actualiza: {disc_domain}"
                        )
                    else:
                        self.insert_into_scraping(row)

                    self.mark_prod_as_processed(row["disc_domain_id"])
                    batch_count += 1

                self.scraping_conn.commit()
                self.prod_conn.commit()
                total_processed += batch_count

                self.logger.info(
                    f"[domain_discovery_from_prod] Batch OK, total sincronizado hasta ahora: {total_processed}"
                )
            except Exception as e:
                self.logger.exception(
                    f"[domain_discovery_from_prod] Error en batch, rollback en ambas bases: {e}"
                )
                self.scraping_conn.rollback()
                self.prod_conn.rollback()
                break

        return total_processed

    def run(self):
        self.logger.info("===== Inicio ETL domain_discovery_from_prod =====")
        self.connect()

        try:
            total = self.process_all()
            self.logger.info(
                f"ETL domain_discovery_from_prod finalizado. "
                f"Total registros sincronizados: {total}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL domain_discovery_from_prod =====")
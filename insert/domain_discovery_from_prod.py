# domain_discovery_from_prod.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomainDiscoveryFromProd:
    """
    Sincroniza public.domain_discovery desde prod_db hacia scraping_db.

    Lógica:
      - PROD:
          * Lee solo registros con processed = false.
      - SCRAPING:
          * UPDATE por disc_domain_id.
          * Si no existe, INSERT.
          * Copia campos de negocio seleccionados.
          * NO actualiza processed, processed_at, online_status ni status_details en scraping.
      - PROD:
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

    # ---------- SCRAPING: upsert por disc_domain_id ----------

    def upsert_into_scraping(self, row: dict):
        """
        Inserta o actualiza una fila en scraping.domain_discovery.

        Clave: disc_domain_id.

        - Si existe -> UPDATE.
        - Si no existe -> INSERT.
        - NO actualiza processed, processed_at, online_status ni status_details en scraping.
        """
        disc_domain_id = row["disc_domain_id"]

        self.logger.debug(
            f"[domain_discovery_from_prod] Upsert en scraping para disc_domain_id={disc_domain_id}"
        )

        update_query = """
            UPDATE public.domain_discovery
            SET
                disc_domain                  = %(disc_domain)s,
                keyword                      = %(keyword)s,
                whois_id                     = %(whois_id)s,
                first_add                    = %(first_add)s,
                new_domain                   = %(new_domain)s,
                age_of_registration          = %(age_of_registration)s,
                estimated_domain_age         = %(estimated_domain_age)s,
                commercial_registrant        = %(commercial_registrant)s,
                likely_commercial_registrant = %(likely_commercial_registrant)s,
                registrar_name               = %(registrar_name)s,
                ping                         = %(ping)s,
                tracert                      = %(tracert)s,
                domain_id                    = %(domain_id)s,
                ml_piracy                    = %(ml_piracy)s,
                site_url                     = %(site_url)s,
                site_domain                  = %(site_domain)s,
                status_msg                   = %(status_msg)s,
                "source"                     = %(source)s,
                tenant                       = %(tenant)s,
                exc_domain_id                = %(exc_domain_id)s,
                disc_domain_root             = %(disc_domain_root)s,
                is_iptv                      = %(is_iptv)s,
                subdomain_host               = %(subdomain_host)s,
                ml_media_type_id             = %(ml_media_type_id)s,
                ml_dd_classification_id      = %(ml_dd_classification_id)s
            WHERE disc_domain_id = %(disc_domain_id)s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(update_query, row)

            if cur.rowcount == 0:
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

                self.logger.debug(
                    f"[domain_discovery_from_prod] INSERT en scraping para disc_domain_id={disc_domain_id}"
                )
                cur.execute(insert_query, row)
            else:
                self.logger.debug(
                    f"[domain_discovery_from_prod] UPDATE aplicado en scraping para disc_domain_id={disc_domain_id}"
                )

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

            try:
                for row in rows:
                    self.upsert_into_scraping(row)
                    self.mark_prod_as_processed(row["disc_domain_id"])
                    total_processed += 1

                self.scraping_conn.commit()
                self.prod_conn.commit()

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
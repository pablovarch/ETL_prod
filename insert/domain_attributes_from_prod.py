# domain_attributes_from_prod.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomainAttributesFromProd:
    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("domain_attributes_from_prod_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.prod_conn = None
        self.scraping_conn = None

    def connect(self):
        self.logger.info("Iniciando conexiones (domain_attributes_from_prod)")
        try:
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.logger.info("Conexiones OK (domain_attributes_from_prod)")
        except Exception as e:
            self.logger.exception(
                f"Error al conectar a las bases (domain_attributes_from_prod): {e}"
            )
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (domain_attributes_from_prod)")
        try:
            if self.prod_conn:
                self.prod_conn.close()
            if self.scraping_conn:
                self.scraping_conn.close()
        except Exception as e:
            self.logger.exception(
                f"Error al cerrar conexiones (domain_attributes_from_prod): {e}"
            )

    def fetch_rows_from_prod(self) -> list[dict]:
        self.logger.info(
            f"[domain_attributes_from_prod] Leyendo desde prod (processed = false, limite={self.batch_size})"
        )

        query = """
            SELECT
                domain_id,
                "domain",
                enforcements_1k,
                enforcements_1k_quality,
                last_seen,
                avg_monthly_traffic,
                domain_classification_id,
                online_status,
                offline_type,
                redirect_domain,
                piracy_brand_id,
                piracy_kw_id,
                type_site_id,
                acceptance_80pct,
                indexed_80pct,
                "general",
                site_url,
                acceptance_cat,
                ad_traffic_cat,
                enforcement_cat,
                enforcements,
                indexed_cat,
                last_enforced_cat,
                site_traffic,
                site_traffic_cat,
                analyst_classification_id,
                analyst_type_site_id,
                offline_type_id,
                first_enf_date,
                first_add_date,
                ace_enforcements,
                updated_by,
                ad_traffic,
                language_id,
                domain_origin,
                status_msg,
                reason,
                ping,
                tracert,
                forum,
                ecommerce,
                calculated_online_status,
                domain_source_ids,
                domain_source_category_id,
                dom_source_cat_id,
                subdomain_host,
                ch_last_seen,
                gtr_last_seen,
                confirmation_date
            FROM public.domain_attributes
            WHERE processed = false
              AND first_add_date >= DATE '2026-05-01'
              AND domain_origin = 'GTR'
            ORDER BY domain_id
            LIMIT %s
        """

        with self.prod_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (self.batch_size,))
            rows = cur.fetchall()

        self.logger.info(
            f"[domain_attributes_from_prod] Filas leídas desde prod: {len(rows)}"
        )
        return rows

    def mark_prod_as_processed(self, domain_id: int):
        self.logger.debug(
            f"[domain_attributes_from_prod] Marcando processed=true en PROD para domain_id={domain_id}"
        )

        query = """
            UPDATE public.domain_attributes
            SET processed = true,
                processed_at = NOW()
            WHERE domain_id = %s
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, (domain_id,))

    def upsert_into_scraping(self, row: dict):
        domain = row["domain"]

        self.logger.debug(
            f"[domain_attributes_from_prod] Upsert en scraping para domain={domain}"
        )

        update_query = """
            UPDATE public.domain_attributes
            SET
                enforcements_1k            = %(enforcements_1k)s,
                enforcements_1k_quality    = %(enforcements_1k_quality)s,
                last_seen                  = %(last_seen)s,
                avg_monthly_traffic        = %(avg_monthly_traffic)s,
                domain_classification_id   = %(domain_classification_id)s,
                online_status              = %(online_status)s,
                offline_type               = %(offline_type)s,
                redirect_domain            = %(redirect_domain)s,
                piracy_brand_id            = %(piracy_brand_id)s,
                piracy_kw_id               = %(piracy_kw_id)s,
                type_site_id               = %(type_site_id)s,
                acceptance_80pct           = %(acceptance_80pct)s,
                indexed_80pct              = %(indexed_80pct)s,
                "general"                  = %(general)s,
                site_url                   = %(site_url)s,
                acceptance_cat             = %(acceptance_cat)s,
                ad_traffic_cat             = %(ad_traffic_cat)s,
                enforcement_cat            = %(enforcement_cat)s,
                enforcements               = %(enforcements)s,
                indexed_cat                = %(indexed_cat)s,
                last_enforced_cat          = %(last_enforced_cat)s,
                site_traffic               = %(site_traffic)s,
                site_traffic_cat           = %(site_traffic_cat)s,
                analyst_classification_id  = %(analyst_classification_id)s,
                analyst_type_site_id       = %(analyst_type_site_id)s,
                offline_type_id            = %(offline_type_id)s,
                first_enf_date             = %(first_enf_date)s,
                first_add_date             = %(first_add_date)s,
                ace_enforcements           = %(ace_enforcements)s,
                updated_by                 = %(updated_by)s,
                ad_traffic                 = %(ad_traffic)s,
                language_id                = %(language_id)s,
                domain_origin              = %(domain_origin)s,
                status_msg                 = %(status_msg)s,
                reason                     = %(reason)s,
                ping                       = %(ping)s,
                tracert                    = %(tracert)s,
                forum                      = %(forum)s,
                ecommerce                  = %(ecommerce)s,
                calculated_online_status   = %(calculated_online_status)s,
                domain_source_ids          = %(domain_source_ids)s,
                domain_source_category_id  = %(domain_source_category_id)s,
                dom_source_cat_id          = %(dom_source_cat_id)s,
                subdomain_host             = %(subdomain_host)s,
                ch_last_seen               = %(ch_last_seen)s,
                gtr_last_seen              = %(gtr_last_seen)s,
                confirmation_date          = %(confirmation_date)s
            WHERE "domain" = %(domain)s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(update_query, row)

            if cur.rowcount == 0:
                insert_query = """
                    INSERT INTO public.domain_attributes (
                        "domain",
                        enforcements_1k,
                        enforcements_1k_quality,
                        last_seen,
                        avg_monthly_traffic,
                        domain_classification_id,
                        online_status,
                        offline_type,
                        redirect_domain,
                        piracy_brand_id,
                        piracy_kw_id,
                        type_site_id,
                        acceptance_80pct,
                        indexed_80pct,
                        "general",
                        site_url,
                        acceptance_cat,
                        ad_traffic_cat,
                        enforcement_cat,
                        enforcements,
                        indexed_cat,
                        last_enforced_cat,
                        site_traffic,
                        site_traffic_cat,
                        analyst_classification_id,
                        analyst_type_site_id,
                        offline_type_id,
                        first_enf_date,
                        first_add_date,
                        ace_enforcements,
                        updated_by,
                        ad_traffic,
                        language_id,
                        domain_origin,
                        status_msg,
                        reason,
                        ping,
                        tracert,
                        forum,
                        ecommerce,
                        calculated_online_status,
                        domain_source_ids,
                        domain_source_category_id,
                        dom_source_cat_id,
                        subdomain_host,
                        ch_last_seen,
                        gtr_last_seen,
                        confirmation_date
                    ) VALUES (
                        %(domain)s,
                        %(enforcements_1k)s,
                        %(enforcements_1k_quality)s,
                        %(last_seen)s,
                        %(avg_monthly_traffic)s,
                        %(domain_classification_id)s,
                        %(online_status)s,
                        %(offline_type)s,
                        %(redirect_domain)s,
                        %(piracy_brand_id)s,
                        %(piracy_kw_id)s,
                        %(type_site_id)s,
                        %(acceptance_80pct)s,
                        %(indexed_80pct)s,
                        %(general)s,
                        %(site_url)s,
                        %(acceptance_cat)s,
                        %(ad_traffic_cat)s,
                        %(enforcement_cat)s,
                        %(enforcements)s,
                        %(indexed_cat)s,
                        %(last_enforced_cat)s,
                        %(site_traffic)s,
                        %(site_traffic_cat)s,
                        %(analyst_classification_id)s,
                        %(analyst_type_site_id)s,
                        %(offline_type_id)s,
                        %(first_enf_date)s,
                        %(first_add_date)s,
                        %(ace_enforcements)s,
                        %(updated_by)s,
                        %(ad_traffic)s,
                        %(language_id)s,
                        %(domain_origin)s,
                        %(status_msg)s,
                        %(reason)s,
                        %(ping)s,
                        %(tracert)s,
                        %(forum)s,
                        %(ecommerce)s,
                        %(calculated_online_status)s,
                        %(domain_source_ids)s,
                        %(domain_source_category_id)s,
                        %(dom_source_cat_id)s,
                        %(subdomain_host)s,
                        %(ch_last_seen)s,
                        %(gtr_last_seen)s,
                        %(confirmation_date)s
                    )
                """

                self.logger.debug(
                    f"[domain_attributes_from_prod] INSERT en scraping para domain={domain}"
                )
                cur.execute(insert_query, row)
            else:
                self.logger.debug(
                    f"[domain_attributes_from_prod] UPDATE aplicado en scraping para domain={domain}"
                )

    def process_all(self) -> int:
        total_processed = 0

        while True:
            rows = self.fetch_rows_from_prod()
            if not rows:
                break

            batch_count = 0
            try:
                for row in rows:
                    self.upsert_into_scraping(row)
                    self.mark_prod_as_processed(row["domain_id"])
                    batch_count += 1

                self.scraping_conn.commit()
                self.prod_conn.commit()
                total_processed += batch_count

                self.logger.info(
                    f"[domain_attributes_from_prod] Batch OK, total sincronizado hasta ahora: {total_processed}"
                )
            except Exception as e:
                self.logger.exception(
                    f"[domain_attributes_from_prod] Error en batch, rollback en ambas bases: {e}"
                )
                self.scraping_conn.rollback()
                self.prod_conn.rollback()
                break

        return total_processed

    def run(self):
        self.logger.info("===== Inicio ETL domain_attributes_from_prod =====")
        self.connect()

        try:
            total = self.process_all()
            self.logger.info(
                f"ETL domain_attributes_from_prod finalizado. Total dominios sincronizados: {total}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL domain_attributes_from_prod =====")
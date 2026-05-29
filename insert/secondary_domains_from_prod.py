import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class SecondaryDomainsFromProd:
    """
    Sync public.secondary_domains from prod_db to scraping_db.

    Rules:
    - Reads rows from prod with processed = false
    - In scraping:
        * only INSERT if sec_domain does not already exist
        * no UPDATE
        * inserts sec_domain_id from prod
    - In prod:
        * marks processed = true after successful handling
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("secondary_domains_from_prod_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.prod_conn = None
        self.scraping_conn = None

    # ---------- Connections ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (secondary_domains_from_prod)")
        try:
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.logger.info("Conexiones OK (secondary_domains_from_prod)")
        except Exception as e:
            self.logger.exception(
                f"Error al conectar a las bases (secondary_domains_from_prod): {e}"
            )
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (secondary_domains_from_prod)")
        try:
            if self.prod_conn:
                self.prod_conn.close()
            if self.scraping_conn:
                self.scraping_conn.close()
        except Exception as e:
            self.logger.exception(
                f"Error al cerrar conexiones (secondary_domains_from_prod): {e}"
            )

    # ---------- PROD ----------

    def fetch_rows_from_prod(self) -> list[dict]:
        """
        Fetch a batch from prod.secondary_domains where processed = false.
        """
        self.logger.info(
            f"[secondary_domains_from_prod] Leyendo desde prod (processed = false, limite={self.batch_size})"
        )

        query = """
            SELECT
                sec_domain_id,
                sec_domain_url,
                sec_domain,
                gtr_history,
                piracy_kw_br,
                google_indexed,
                ad_content,
                blocks_direct_browsing,
                specific_sw_category,
                referral_traffic,
                secondary_domain_type,
                redirect_domain,
                short_html,
                html_length,
                collection_timestamp,
                ml_secondary_domain_type,
                added,
                sec_domain_root,
                exc_domain_id,
                whois_id,
                domain_id,
                sec_domain_source,
                ml_piracy,
                ml_sec_domain_classification,
                an_sec_domain_classification,
                online_status,
                ad_count,
                site_map_count,
                tld_poor,
                site_traffic,
                graymarket_label,
                has_affiliate_handoff,
                is_ecommerce,
                ssl_poor,
                mfa_engagement,
                high_traffic,
                ad_density,
                redirect_status_code,
                is_high_risk_geo,
                ml_model_classified,
                review_status,
                publication_status,
                suspect_cloak,
                last_seen,
                abuse_category,
                confidence,
                justification,
                retraction_reason,
                first_reported,
                fraud_type,
                exploit_type,
                content_type,
                top_targets,
                recommended_action_id,
                previous_action_id,
                top_referrers,
                google_search_results,
                retracted,
                decision_source,
                updated_by
            FROM public.secondary_domains
            WHERE processed = false
            ORDER BY sec_domain_id
            LIMIT %s
        """

        with self.prod_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (self.batch_size,))
            rows = cur.fetchall()

        self.logger.info(
            f"[secondary_domains_from_prod] Filas leídas desde prod: {len(rows)}"
        )
        return rows

    def mark_prod_as_processed(self, sec_domain_id: int):
        """
        Mark prod row as processed.
        """
        self.logger.debug(
            f"[secondary_domains_from_prod] Marcando processed=true en PROD para sec_domain_id={sec_domain_id}"
        )

        query = """
            UPDATE public.secondary_domains
            SET processed = true,
                processed_at = NOW()
            WHERE sec_domain_id = %s
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, (sec_domain_id,))

    # ---------- SCRAPING ----------

    def exists_in_scraping(self, sec_domain: str) -> bool:
        """
        Check if sec_domain already exists in scraping.
        """
        query = """
            SELECT 1
            FROM public.secondary_domains
            WHERE sec_domain = %s
            LIMIT 1
        """
        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (sec_domain,))
            return cur.fetchone() is not None

    def insert_into_scraping(self, row: dict):
        """
        Insert a new row into scraping copying sec_domain_id from prod.
        """
        sec_domain = row["sec_domain"]
        sec_domain_id = row["sec_domain_id"]

        self.logger.debug(
            f"[secondary_domains_from_prod] INSERT en scraping para sec_domain={sec_domain}, sec_domain_id={sec_domain_id}"
        )

        insert_query = """
            INSERT INTO public.secondary_domains (
                sec_domain_id,
                sec_domain_url,
                sec_domain,
                gtr_history,
                piracy_kw_br,
                google_indexed,
                ad_content,
                blocks_direct_browsing,
                specific_sw_category,
                referral_traffic,
                secondary_domain_type,
                redirect_domain,
                short_html,
                html_length,
                collection_timestamp,
                ml_secondary_domain_type,
                added,
                sec_domain_root,
                exc_domain_id,
                whois_id,
                domain_id,
                sec_domain_source,
                ml_piracy,
                ml_sec_domain_classification,
                an_sec_domain_classification,
                online_status,
                ad_count,
                site_map_count,
                tld_poor,
                site_traffic,
                graymarket_label,
                has_affiliate_handoff,
                is_ecommerce,
                ssl_poor,
                mfa_engagement,
                high_traffic,
                ad_density,
                redirect_status_code,
                is_high_risk_geo,
                ml_model_classified,
                review_status,
                publication_status,
                suspect_cloak,
                last_seen,
                abuse_category,
                confidence,
                justification,
                retraction_reason,
                first_reported,
                fraud_type,
                exploit_type,
                content_type,
                top_targets,
                recommended_action_id,
                previous_action_id,
                top_referrers,
                google_search_results,
                retracted,
                decision_source,
                updated_by
            ) VALUES (
                %(sec_domain_id)s,
                %(sec_domain_url)s,
                %(sec_domain)s,
                %(gtr_history)s,
                %(piracy_kw_br)s,
                %(google_indexed)s,
                %(ad_content)s,
                %(blocks_direct_browsing)s,
                %(specific_sw_category)s,
                %(referral_traffic)s,
                %(secondary_domain_type)s,
                %(redirect_domain)s,
                %(short_html)s,
                %(html_length)s,
                %(collection_timestamp)s,
                %(ml_secondary_domain_type)s,
                %(added)s,
                %(sec_domain_root)s,
                %(exc_domain_id)s,
                %(whois_id)s,
                %(domain_id)s,
                %(sec_domain_source)s,
                %(ml_piracy)s,
                %(ml_sec_domain_classification)s,
                %(an_sec_domain_classification)s,
                %(online_status)s,
                %(ad_count)s,
                %(site_map_count)s,
                %(tld_poor)s,
                %(site_traffic)s,
                %(graymarket_label)s,
                %(has_affiliate_handoff)s,
                %(is_ecommerce)s,
                %(ssl_poor)s,
                %(mfa_engagement)s,
                %(high_traffic)s,
                %(ad_density)s,
                %(redirect_status_code)s,
                %(is_high_risk_geo)s,
                %(ml_model_classified)s,
                %(review_status)s,
                %(publication_status)s,
                %(suspect_cloak)s,
                %(last_seen)s,
                %(abuse_category)s,
                %(confidence)s,
                %(justification)s,
                %(retraction_reason)s,
                %(first_reported)s,
                %(fraud_type)s,
                %(exploit_type)s,
                %(content_type)s,
                %(top_targets)s,
                %(recommended_action_id)s,
                %(previous_action_id)s,
                %(top_referrers)s,
                %(google_search_results)s,
                %(retracted)s,
                %(decision_source)s,
                %(updated_by)s
            )
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(insert_query, row)

    # ---------- Orchestration ----------

    def process_all(self) -> int:
        total_processed = 0

        while True:
            rows = self.fetch_rows_from_prod()
            if not rows:
                break

            batch_count = 0
            try:
                for row in rows:
                    sec_domain = row["sec_domain"]

                    if self.exists_in_scraping(sec_domain):
                        self.logger.info(
                            f"[secondary_domains_from_prod] sec_domain ya existe en scraping, no se actualiza: {sec_domain}"
                        )
                    else:
                        self.insert_into_scraping(row)

                    self.mark_prod_as_processed(row["sec_domain_id"])
                    batch_count += 1

                self.scraping_conn.commit()
                self.prod_conn.commit()
                total_processed += batch_count

                self.logger.info(
                    f"[secondary_domains_from_prod] Batch OK, total sincronizado hasta ahora: {total_processed}"
                )
            except Exception as e:
                self.logger.exception(
                    f"[secondary_domains_from_prod] Error en batch, rollback en ambas bases: {e}"
                )
                self.scraping_conn.rollback()
                self.prod_conn.rollback()
                break

        return total_processed

    def run(self):
        self.logger.info("===== Inicio ETL secondary_domains_from_prod =====")
        self.connect()

        try:
            total = self.process_all()
            self.logger.info(
                f"ETL secondary_domains_from_prod finalizado. "
                f"Total registros sincronizados: {total}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL secondary_domains_from_prod =====")
# domain_attributes_from_prod.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomainAttributesFromProd:
    """
    Sincroniza public.domain_attributes desde prod_db hacia scraping_db.

    Lógica:
      - En PROD:
          * Lee solo registros con processed = false.
      - En SCRAPING:
          * UPDATE por "domain" (clave lógica).
          * Si no existe el domain, INSERT.
          * Copia todos los campos de negocio.
          * processed = true, processed_at = NOW() (para que NO salgan a prod).
      - En PROD:
          * Después de sincronizar, marca processed = true, processed_at = NOW()
            para ese domain_id.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("domain_attributes_from_prod_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.prod_conn = None
        self.scraping_conn = None

    # ---------- Conexiones ----------

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

    # ---------- PROD: leer y marcar processed ----------

    def fetch_rows_from_prod(self) -> list[dict]:
        """
        Trae un batch de filas desde prod.domain_attributes con processed = false.
        """
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
                ml_domain_classification_id,
                ml_media_type_id,
                an_media_type_id,
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
                confirmation_date,
                ml_domain_classification_v2_id,
                ml_media_type_v2_id
            FROM public.domain_attributes
            WHERE processed = false
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
        """
        Marca processed = true en prod.domain_attributes para ese domain_id.
        """
        self.logger.debug(
            f"[domain_attributes_from_prod] Marcando processed=true en PROD para domain_id={domain_id}"
        )

        query = """
            UPDATE public.domain_attributes
            SET processed    = true,
                processed_at = NOW()
            WHERE domain_id = %s
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, (domain_id,))

    # ---------- SCRAPING: upsert por domain ----------

    def upsert_into_scraping(self, row: dict):
        """
        Inserta o actualiza una fila en scraping.domain_attributes.

        Clave de negocio: "domain".

        - Si existe el domain -> UPDATE.
        - Si no existe -> INSERT (sin tocar domain_id, lo genera la secuencia).
        - Copia todos los campos de negocio.
        - Siempre marca processed = true, processed_at = NOW().
        """
        domain = row["domain"]

        self.logger.debug(
            f"[domain_attributes_from_prod] Upsert en scraping para domain={domain}"
        )

        # ---------- UPDATE por "domain" ----------
        update_query = """
            UPDATE public.domain_attributes
            SET
                enforcements_1k              = %s,
                enforcements_1k_quality      = %s,
                last_seen                    = %s,
                avg_monthly_traffic          = %s,
                domain_classification_id     = %s,
                online_status                = %s,
                offline_type                 = %s,
                redirect_domain              = %s,
                piracy_brand_id              = %s,
                piracy_kw_id                 = %s,
                type_site_id                 = %s,
                acceptance_80pct             = %s,
                indexed_80pct                = %s,
                "general"                    = %s,
                site_url                     = %s,
                acceptance_cat               = %s,
                ad_traffic_cat               = %s,
                enforcement_cat              = %s,
                enforcements                 = %s,
                indexed_cat                  = %s,
                last_enforced_cat            = %s,
                site_traffic                 = %s,
                site_traffic_cat             = %s,
                analyst_classification_id    = %s,
                analyst_type_site_id         = %s,
                offline_type_id              = %s,
                first_enf_date               = %s,
                first_add_date               = %s,
                ace_enforcements             = %s,
                ml_domain_classification_id  = %s,
                ml_media_type_id             = %s,
                an_media_type_id             = %s,
                updated_by                   = %s,
                ad_traffic                   = %s,
                language_id                  = %s,
                domain_origin                = %s,
                status_msg                   = %s,
                reason                       = %s,
                ping                         = %s,
                tracert                      = %s,
                forum                        = %s,
                ecommerce                    = %s,
                calculated_online_status     = %s,
                domain_source_ids            = %s,
                domain_source_category_id    = %s,
                dom_source_cat_id            = %s,
                subdomain_host               = %s,
                ch_last_seen                 = %s,
                gtr_last_seen                = %s,
                confirmation_date            = %s,
                ml_domain_classification_v2_id = %s,
                ml_media_type_v2_id          = %s,
                processed                    = true,
                processed_at                 = NOW()
            WHERE "domain" = %s
        """

        update_params = (
            row["enforcements_1k"],
            row["enforcements_1k_quality"],
            row["last_seen"],
            row["avg_monthly_traffic"],
            row["domain_classification_id"],
            row["online_status"],
            row["offline_type"],
            row["redirect_domain"],
            row["piracy_brand_id"],
            row["piracy_kw_id"],
            row["type_site_id"],
            row["acceptance_80pct"],
            row["indexed_80pct"],
            row["general"],
            row["site_url"],
            row["acceptance_cat"],
            row["ad_traffic_cat"],
            row["enforcement_cat"],
            row["enforcements"],
            row["indexed_cat"],
            row["last_enforced_cat"],
            row["site_traffic"],
            row["site_traffic_cat"],
            row["analyst_classification_id"],
            row["analyst_type_site_id"],
            row["offline_type_id"],
            row["first_enf_date"],
            row["first_add_date"],
            row["ace_enforcements"],
            row["ml_domain_classification_id"],
            row["ml_media_type_id"],
            row["an_media_type_id"],
            row["updated_by"],
            row["ad_traffic"],
            row["language_id"],
            row["domain_origin"],
            row["status_msg"],
            row["reason"],
            row["ping"],
            row["tracert"],
            row["forum"],
            row["ecommerce"],
            row["calculated_online_status"],
            row["domain_source_ids"],
            row["domain_source_category_id"],
            row["dom_source_cat_id"],
            row["subdomain_host"],
            row["ch_last_seen"],
            row["gtr_last_seen"],
            row["confirmation_date"],
            row["ml_domain_classification_v2_id"],
            row["ml_media_type_v2_id"],
            domain,
        )

        with self.scraping_conn.cursor() as cur:
            cur.execute(update_query, update_params)

            if cur.rowcount == 0:
                # ---------- INSERT si el dominio no existe ----------
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
                        ml_domain_classification_id,
                        ml_media_type_id,
                        an_media_type_id,
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
                        confirmation_date,
                        ml_domain_classification_v2_id,
                        ml_media_type_v2_id,
                        processed,
                        processed_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, true, NOW()
                    )
                """

                insert_params = (
                    domain,
                    row["enforcements_1k"],
                    row["enforcements_1k_quality"],
                    row["last_seen"],
                    row["avg_monthly_traffic"],
                    row["domain_classification_id"],
                    row["online_status"],
                    row["offline_type"],
                    row["redirect_domain"],
                    row["piracy_brand_id"],
                    row["piracy_kw_id"],
                    row["type_site_id"],
                    row["acceptance_80pct"],
                    row["indexed_80pct"],
                    row["general"],
                    row["site_url"],
                    row["acceptance_cat"],
                    row["ad_traffic_cat"],
                    row["enforcement_cat"],
                    row["enforcements"],
                    row["indexed_cat"],
                    row["last_enforced_cat"],
                    row["site_traffic"],
                    row["site_traffic_cat"],
                    row["analyst_classification_id"],
                    row["analyst_type_site_id"],
                    row["offline_type_id"],
                    row["first_enf_date"],
                    row["first_add_date"],
                    row["ace_enforcements"],
                    row["ml_domain_classification_id"],
                    row["ml_media_type_id"],
                    row["an_media_type_id"],
                    row["updated_by"],
                    row["ad_traffic"],
                    row["language_id"],
                    row["domain_origin"],
                    row["status_msg"],
                    row["reason"],
                    row["ping"],
                    row["tracert"],
                    row["forum"],
                    row["ecommerce"],
                    row["calculated_online_status"],
                    row["domain_source_ids"],
                    row["domain_source_category_id"],
                    row["dom_source_cat_id"],
                    row["subdomain_host"],
                    row["ch_last_seen"],
                    row["gtr_last_seen"],
                    row["confirmation_date"],
                    row["ml_domain_classification_v2_id"],
                    row["ml_media_type_v2_id"],
                )

                self.logger.debug(
                    f"[domain_attributes_from_prod] INSERT en scraping para domain={domain}"
                )
                cur.execute(insert_query, insert_params)
            else:
                self.logger.debug(
                    f"[domain_attributes_from_prod] UPDATE aplicado en scraping para domain={domain}"
                )

    # ---------- Orquestación ----------

    def process_all(self) -> int:
        """
        Recorre prod.domain_attributes en batches (processed = false)
        y sincroniza hacia scraping.
        """
        total_processed = 0

        while True:
            rows = self.fetch_rows_from_prod()
            if not rows:
                break

            try:
                for row in rows:
                    # row ya viene como dict con todas las claves
                    self.upsert_into_scraping(row)
                    self.mark_prod_as_processed(row["domain_id"])
                    total_processed += 1

                # Commit de ambos lados por batch
                self.scraping_conn.commit()
                self.prod_conn.commit()

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
                f"ETL domain_attributes_from_prod finalizado. "
                f"Total dominios sincronizados: {total}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL domain_attributes_from_prod =====")


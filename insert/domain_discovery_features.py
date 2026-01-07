# domain_discovery_features_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class DomainDiscoveryFeaturesSync:
    """
    Sincroniza public.domain_discovery_features desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.domain_discovery_features.
      - Hace UPSERT en prod.domain_discovery_features usando ON CONFLICT(html_feature_id).
      - Marca processed = true, processed_at = NOW() en scraping.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("domain_discovery_features_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (domain_discovery_features_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (domain_discovery_features_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (domain_discovery_features_sync)")
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
        Filas pendientes en scraping.domain_discovery_features (processed = false).
        """
        self.logger.info(
            f"[domain_discovery_features_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                html_feature_id,
                avg_depth,
                max_depth,
                node_count,
                inline_script_count,
                inline_script_bytes,
                has_schema,
                total_schema_blocks,
                schema_types_count,
                asn_age,
                ip_country,
                is_high_risk_geo,
                cookie_wall_ratio,
                overlay_count,
                feature_added,
                domain_id,
                exc_domain_id,
                disc_domain_id,
                performance_score,
                largest_contentful_paint,
                cumulative_layout_shift,
                piracy,
                div,
                a,
                text_length,
                img,
                span,
                li,
                script,
                link,
                meta,
                p,
                length_html,
                count_ad_script_src,
                domain_name,
                sec_domain_id
            FROM public.domain_discovery_features
            WHERE processed = false
            ORDER BY html_feature_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[domain_discovery_features_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.domain_discovery_features usando html_feature_id como clave.
        """
        self.logger.debug(
            f"[domain_discovery_features_sync] Upsert prod para html_feature_id={row.get('html_feature_id')}"
        )

        query = """
            INSERT INTO public.domain_discovery_features (
                html_feature_id,
                avg_depth,
                max_depth,
                node_count,
                inline_script_count,
                inline_script_bytes,
                has_schema,
                total_schema_blocks,
                schema_types_count,
                asn_age,
                ip_country,
                is_high_risk_geo,
                cookie_wall_ratio,
                overlay_count,
                feature_added,
                domain_id,
                exc_domain_id,
                disc_domain_id,
                performance_score,
                largest_contentful_paint,
                cumulative_layout_shift,
                piracy,
                div,
                a,
                text_length,
                img,
                span,
                li,
                script,
                link,
                meta,
                p,
                length_html,
                count_ad_script_src,
                domain_name,
                sec_domain_id
            ) VALUES (
                %(html_feature_id)s,
                %(avg_depth)s,
                %(max_depth)s,
                %(node_count)s,
                %(inline_script_count)s,
                %(inline_script_bytes)s,
                %(has_schema)s,
                %(total_schema_blocks)s,
                %(schema_types_count)s,
                %(asn_age)s,
                %(ip_country)s,
                %(is_high_risk_geo)s,
                %(cookie_wall_ratio)s,
                %(overlay_count)s,
                %(feature_added)s,
                %(domain_id)s,
                %(exc_domain_id)s,
                %(disc_domain_id)s,
                %(performance_score)s,
                %(largest_contentful_paint)s,
                %(cumulative_layout_shift)s,
                %(piracy)s,
                %(div)s,
                %(a)s,
                %(text_length)s,
                %(img)s,
                %(span)s,
                %(li)s,
                %(script)s,
                %(link)s,
                %(meta)s,
                %(p)s,
                %(length_html)s,
                %(count_ad_script_src)s,
                %(domain_name)s,
                %(sec_domain_id)s
            )
            ON CONFLICT (html_feature_id) DO UPDATE SET
                avg_depth               = EXCLUDED.avg_depth,
                max_depth               = EXCLUDED.max_depth,
                node_count              = EXCLUDED.node_count,
                inline_script_count     = EXCLUDED.inline_script_count,
                inline_script_bytes     = EXCLUDED.inline_script_bytes,
                has_schema              = EXCLUDED.has_schema,
                total_schema_blocks     = EXCLUDED.total_schema_blocks,
                schema_types_count      = EXCLUDED.schema_types_count,
                asn_age                 = EXCLUDED.asn_age,
                ip_country              = EXCLUDED.ip_country,
                is_high_risk_geo        = EXCLUDED.is_high_risk_geo,
                cookie_wall_ratio       = EXCLUDED.cookie_wall_ratio,
                overlay_count           = EXCLUDED.overlay_count,
                feature_added           = EXCLUDED.feature_added,
                domain_id               = EXCLUDED.domain_id,
                exc_domain_id           = EXCLUDED.exc_domain_id,
                disc_domain_id          = EXCLUDED.disc_domain_id,
                performance_score       = EXCLUDED.performance_score,
                largest_contentful_paint = EXCLUDED.largest_contentful_paint,
                cumulative_layout_shift = EXCLUDED.cumulative_layout_shift,
                piracy                  = EXCLUDED.piracy,
                div                     = EXCLUDED.div,
                a                       = EXCLUDED.a,
                text_length             = EXCLUDED.text_length,
                img                     = EXCLUDED.img,
                span                    = EXCLUDED.span,
                li                      = EXCLUDED.li,
                script                  = EXCLUDED.script,
                link                    = EXCLUDED.link,
                meta                    = EXCLUDED.meta,
                p                       = EXCLUDED.p,
                length_html             = EXCLUDED.length_html,
                count_ad_script_src     = EXCLUDED.count_ad_script_src,
                domain_name             = EXCLUDED.domain_name,
                sec_domain_id           = EXCLUDED.sec_domain_id;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, html_feature_id: int):
        """
        Marca processed=true en scraping.domain_discovery_features.
        """
        self.logger.debug(
            f"[domain_discovery_features_sync] Marcando processed=true para html_feature_id={html_feature_id}"
        )

        query = """
            UPDATE public.domain_discovery_features
            SET processed    = true,
                processed_at = NOW()
            WHERE html_feature_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (html_feature_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)
        if not rows:
            self.logger.info(
                "[domain_discovery_features_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[domain_discovery_features_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["html_feature_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.logger.info(
                f"[domain_discovery_features_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[domain_discovery_features_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL domain_discovery_features_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL domain_discovery_features_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL domain_discovery_features_sync =====")

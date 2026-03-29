# browser_profiles_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class BrowserProfilesSync:
    """
    Sincroniza public.browser_profiles desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.browser_profiles.
      - UPSERT en prod.browser_profiles usando ON CONFLICT(browser_profile_id).
      - Marca processed = true, processed_at = NOW() en scraping.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("browser_profiles_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (browser_profiles_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (browser_profiles_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (browser_profiles_sync)")
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
        Filas pendientes en scraping.browser_profiles (processed = false).
        """
        self.logger.info(
            f"[browser_profiles_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                browser_profile_id,
                driver,
                profile,
                proxy_zone,
                crawler_method,
                os,
                user_agent_string,
                country_id,
                proxy_service,
                proxy_product,
                ip_type,
                ip,
                accept_language,
                referer,
                navigator_platform,
                navigator_cores,
                navigator_ram,
                navigator_language,
                navigator_bot_detection,
                screen_width,
                screen_height,
                screen_color_depth,
                window_pixel_ratio,
                timezone,
                timezone_offset,
                canvas_hash,
                webgl_vendor_renderer,
                webgl_extensions,
                fonts,
                audio_hash,
                touch_points,
                navigator_media_devices,
                navigator_plugins,
                storage_availability,
                indexed_db_availability,
                navigator_battery,
                connection_type,
                supported_codecs,
                device_ram,
                device_cores,
                bot_user
            FROM public.browser_profiles
            WHERE processed = false
            ORDER BY browser_profile_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[browser_profiles_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.browser_profiles usando browser_profile_id como clave.
        """
        self.logger.debug(
            f"[browser_profiles_sync] Upsert prod para browser_profile_id={row.get('browser_profile_id')}"
        )

        query = """
            INSERT INTO public.browser_profiles (
                browser_profile_id,
                driver,
                profile,
                proxy_zone,
                crawler_method,
                os,
                user_agent_string,
                country_id,
                proxy_service,
                proxy_product,
                ip_type,
                ip,
                accept_language,
                referer,
                navigator_platform,
                navigator_cores,
                navigator_ram,
                navigator_language,
                navigator_bot_detection,
                screen_width,
                screen_height,
                screen_color_depth,
                window_pixel_ratio,
                timezone,
                timezone_offset,
                canvas_hash,
                webgl_vendor_renderer,
                webgl_extensions,
                fonts,
                audio_hash,
                touch_points,
                navigator_media_devices,
                navigator_plugins,
                storage_availability,
                indexed_db_availability,
                navigator_battery,
                connection_type,
                supported_codecs,
                device_ram,
                device_cores,
                bot_user
            ) VALUES (
                %(browser_profile_id)s,
                %(driver)s,
                %(profile)s,
                %(proxy_zone)s,
                %(crawler_method)s,
                %(os)s,
                %(user_agent_string)s,
                %(country_id)s,
                %(proxy_service)s,
                %(proxy_product)s,
                %(ip_type)s,
                %(ip)s,
                %(accept_language)s,
                %(referer)s,
                %(navigator_platform)s,
                %(navigator_cores)s,
                %(navigator_ram)s,
                %(navigator_language)s,
                %(navigator_bot_detection)s,
                %(screen_width)s,
                %(screen_height)s,
                %(screen_color_depth)s,
                %(window_pixel_ratio)s,
                %(timezone)s,
                %(timezone_offset)s,
                %(canvas_hash)s,
                %(webgl_vendor_renderer)s,
                %(webgl_extensions)s,
                %(fonts)s,
                %(audio_hash)s,
                %(touch_points)s,
                %(navigator_media_devices)s,
                %(navigator_plugins)s,
                %(storage_availability)s,
                %(indexed_db_availability)s,
                %(navigator_battery)s,
                %(connection_type)s,
                %(supported_codecs)s,
                %(device_ram)s,
                %(device_cores)s,
                %(bot_user)s
            )
            ON CONFLICT (browser_profile_id) DO UPDATE SET
                driver                  = EXCLUDED.driver,
                profile                 = EXCLUDED.profile,
                proxy_zone              = EXCLUDED.proxy_zone,
                crawler_method          = EXCLUDED.crawler_method,
                os                      = EXCLUDED.os,
                user_agent_string       = EXCLUDED.user_agent_string,
                country_id              = EXCLUDED.country_id,
                proxy_service           = EXCLUDED.proxy_service,
                proxy_product           = EXCLUDED.proxy_product,
                ip_type                 = EXCLUDED.ip_type,
                ip                      = EXCLUDED.ip,
                accept_language         = EXCLUDED.accept_language,
                referer                 = EXCLUDED.referer,
                navigator_platform      = EXCLUDED.navigator_platform,
                navigator_cores         = EXCLUDED.navigator_cores,
                navigator_ram           = EXCLUDED.navigator_ram,
                navigator_language      = EXCLUDED.navigator_language,
                navigator_bot_detection = EXCLUDED.navigator_bot_detection,
                screen_width            = EXCLUDED.screen_width,
                screen_height           = EXCLUDED.screen_height,
                screen_color_depth      = EXCLUDED.screen_color_depth,
                window_pixel_ratio      = EXCLUDED.window_pixel_ratio,
                timezone                = EXCLUDED.timezone,
                timezone_offset         = EXCLUDED.timezone_offset,
                canvas_hash             = EXCLUDED.canvas_hash,
                webgl_vendor_renderer   = EXCLUDED.webgl_vendor_renderer,
                webgl_extensions        = EXCLUDED.webgl_extensions,
                fonts                   = EXCLUDED.fonts,
                audio_hash              = EXCLUDED.audio_hash,
                touch_points            = EXCLUDED.touch_points,
                navigator_media_devices = EXCLUDED.navigator_media_devices,
                navigator_plugins       = EXCLUDED.navigator_plugins,
                storage_availability    = EXCLUDED.storage_availability,
                indexed_db_availability = EXCLUDED.indexed_db_availability,
                navigator_battery       = EXCLUDED.navigator_battery,
                connection_type         = EXCLUDED.connection_type,
                supported_codecs        = EXCLUDED.supported_codecs,
                device_ram              = EXCLUDED.device_ram,
                device_cores            = EXCLUDED.device_cores,
                bot_user                = EXCLUDED.bot_user;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, browser_profile_id: int):
        """
        Marca processed=true en scraping.browser_profiles.
        """
        self.logger.debug(
            f"[browser_profiles_sync] Marcando processed=true para browser_profile_id={browser_profile_id}"
        )

        query = """
            UPDATE public.browser_profiles
            SET processed = true,
                processed_at = NOW()
            WHERE browser_profile_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (browser_profile_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)

        if not rows:
            self.logger.info(
                "[browser_profiles_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[browser_profiles_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            for row in rows:
                self.upsert_into_prod(row)
                self.mark_as_processed(row["browser_profile_id"])

            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[browser_profiles_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[browser_profiles_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL browser_profiles_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL browser_profiles_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL browser_profiles_sync =====")
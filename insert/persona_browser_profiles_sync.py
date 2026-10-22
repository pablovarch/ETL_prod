# persona_browser_profiles_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class PersonaBrowserProfilesSync:
    """
    Sincroniza public.persona_browser_profiles (perfil de navegador de cada instancia (SIN storage_state: solo vive en scraping)) desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.persona_browser_profiles.
      - UPSERT en prod.persona_browser_profiles usando ON CONFLICT(browser_profile_id); solo pisa la
        fila de prod si la que llega es igual o más nueva (updated_at).
      - Marca processed = true, processed_at = NOW() en scraping SOLO si la
        fila no cambió mientras se copiaba (mismo updated_at que se leyó).
        Si cambió, queda pendiente y se vuelve a copiar en el próximo ciclo.
      - Si un batch falla, se reintenta fila por fila: la fila que falla se
        loguea y queda pendiente para el próximo ciclo, sin frenar al resto.

    En scraping, un trigger vuelve a poner processed = false (y actualiza
    updated_at) cada vez que el cultivo modifica una fila.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("persona_browser_profiles_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None
        # Cursor de la corrida (ultima clave leida): avanza aunque haya filas
        # que fallen o cambien, que quedan pendientes para el próximo ciclo.
        self.last_key = (0,)
        self.synced = 0
        self.changed = 0
        self.failed = 0

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (persona_browser_profiles_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (persona_browser_profiles_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (persona_browser_profiles_sync)")
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
        Filas pendientes en scraping.persona_browser_profiles (processed = false).
        """
        self.logger.info(
            f"[persona_browser_profiles_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                browser_profile_id,
                persona_instance_id,
                pinned_ip,
                locale,
                timezone_id,
                user_agent,
                viewport_width,
                viewport_height,
                cookie_count,
                domain_count,
                created_at,
                last_cultivated_at,
                updated_at
            FROM public.persona_browser_profiles
            WHERE processed = false
              AND (browser_profile_id) > %s
            ORDER BY browser_profile_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (self.last_key, limit))
            rows = cur.fetchall()

        self.logger.info(
            f"[persona_browser_profiles_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.persona_browser_profiles usando browser_profile_id como clave. No pisa una
        versión más nueva que ya esté en prod.
        """
        self.logger.debug(
            f"[persona_browser_profiles_sync] Upsert prod para browser_profile_id={row.get('browser_profile_id')}"
        )

        query = """
            INSERT INTO public.persona_browser_profiles (
                browser_profile_id,
                persona_instance_id,
                pinned_ip,
                locale,
                timezone_id,
                user_agent,
                viewport_width,
                viewport_height,
                cookie_count,
                domain_count,
                created_at,
                last_cultivated_at,
                updated_at
            ) VALUES (
                %(browser_profile_id)s,
                %(persona_instance_id)s,
                %(pinned_ip)s,
                %(locale)s,
                %(timezone_id)s,
                %(user_agent)s,
                %(viewport_width)s,
                %(viewport_height)s,
                %(cookie_count)s,
                %(domain_count)s,
                %(created_at)s,
                %(last_cultivated_at)s,
                %(updated_at)s
            )
            ON CONFLICT (browser_profile_id) DO UPDATE SET
                persona_instance_id = EXCLUDED.persona_instance_id,
                pinned_ip           = EXCLUDED.pinned_ip,
                locale              = EXCLUDED.locale,
                timezone_id         = EXCLUDED.timezone_id,
                user_agent          = EXCLUDED.user_agent,
                viewport_width      = EXCLUDED.viewport_width,
                viewport_height     = EXCLUDED.viewport_height,
                cookie_count        = EXCLUDED.cookie_count,
                domain_count        = EXCLUDED.domain_count,
                created_at          = EXCLUDED.created_at,
                last_cultivated_at  = EXCLUDED.last_cultivated_at,
                updated_at          = EXCLUDED.updated_at
            WHERE public.persona_browser_profiles.updated_at <= EXCLUDED.updated_at;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, row: dict) -> bool:
        """
        Marca processed=true en scraping.persona_browser_profiles, solo si la fila no cambió
        desde que se leyó. Devuelve False si cambió (queda pendiente).
        """
        self.logger.debug(
            f"[persona_browser_profiles_sync] Marcando processed=true para browser_profile_id={row.get('browser_profile_id')}"
        )

        query = """
            UPDATE public.persona_browser_profiles
            SET processed = true,
                processed_at = NOW()
            WHERE browser_profile_id = %(browser_profile_id)s
              AND updated_at = %(updated_at)s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, row)
            return cur.rowcount == 1

    # ---------- Lógica de batch ----------

    def sync_row(self, row: dict) -> bool:
        """Copia una fila a prod y la marca. Devuelve False si cambió durante la copia."""
        self.upsert_into_prod(row)
        return self.mark_as_processed(row)

    def process_rows_one_by_one(self, rows: list[dict]):
        """
        Reintento de un batch que falló: cada fila en su propia transacción.
        La que falla se loguea y queda pendiente (processed = false) para el
        próximo ciclo, sin frenar al resto.
        """
        for row in rows:
            try:
                if self.sync_row(row):
                    self.synced += 1
                else:
                    self.changed += 1
                self.prod_conn.commit()
                self.scraping_conn.commit()
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                raise
            except Exception as e:
                self.prod_conn.rollback()
                self.scraping_conn.rollback()
                self.failed += 1
                self.logger.error(
                    f"[persona_browser_profiles_sync] Fila salteada (browser_profile_id={row.get('browser_profile_id')}), "
                    f"queda pendiente para el próximo ciclo: {e}"
                )

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)

        if not rows:
            self.logger.info(
                "[persona_browser_profiles_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[persona_browser_profiles_sync] Procesando batch de {len(rows)} filas"
        )
        self.last_key = (rows[-1]["browser_profile_id"],)

        try:
            changed = 0
            for row in rows:
                if not self.sync_row(row):
                    changed += 1

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.synced += len(rows) - changed
            self.changed += changed

            self.logger.info(
                f"[persona_browser_profiles_sync] Batch OK ({len(rows)} filas procesadas"
                + (f", {changed} cambiaron durante la copia y quedan pendientes" if changed else "")
                + ")"
            )
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            # Problema de conexión: no tiene sentido reintentar fila por fila.
            raise
        except Exception as e:
            self.logger.warning(
                f"[persona_browser_profiles_sync] Error en batch, rollback y reintento fila por fila: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()
            self.process_rows_one_by_one(rows)

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL persona_browser_profiles_sync =====")
        self.connect()

        try:
            while self.process_batch():
                pass

            self.logger.info(
                f"ETL persona_browser_profiles_sync finalizado. "
                f"Filas sincronizadas: {self.synced}, "
                f"cambiaron durante la copia: {self.changed}, "
                f"con error: {self.failed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL persona_browser_profiles_sync =====")

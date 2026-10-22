# warmup_domains_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class WarmupDomainsSync:
    """
    Sincroniza public.warmup_domains (catalogo de dominios del cultivo) desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.warmup_domains.
      - UPSERT en prod.warmup_domains usando ON CONFLICT(warmup_domain_id); solo pisa la
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
        self.logger = Log.get_logger("warmup_domains_sync_etl")
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
        self.logger.info("Iniciando conexiones (warmup_domains_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (warmup_domains_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (warmup_domains_sync)")
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
        Filas pendientes en scraping.warmup_domains (processed = false).
        """
        self.logger.info(
            f"[warmup_domains_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                warmup_domain_id,
                domain,
                country_key,
                industry_raw,
                industry_category,
                industry_subcategory,
                warmup_interest,
                monthly_visits,
                yoy_traffic_change,
                source,
                imported_at,
                active,
                excluded_reason,
                fail_count,
                last_fail_reason,
                last_fail_at,
                last_ok_at,
                updated_at
            FROM public.warmup_domains
            WHERE processed = false
              AND (warmup_domain_id) > %s
            ORDER BY warmup_domain_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (self.last_key, limit))
            rows = cur.fetchall()

        self.logger.info(
            f"[warmup_domains_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.warmup_domains usando warmup_domain_id como clave. No pisa una
        versión más nueva que ya esté en prod.
        """
        self.logger.debug(
            f"[warmup_domains_sync] Upsert prod para warmup_domain_id={row.get('warmup_domain_id')}"
        )

        query = """
            INSERT INTO public.warmup_domains (
                warmup_domain_id,
                domain,
                country_key,
                industry_raw,
                industry_category,
                industry_subcategory,
                warmup_interest,
                monthly_visits,
                yoy_traffic_change,
                source,
                imported_at,
                active,
                excluded_reason,
                fail_count,
                last_fail_reason,
                last_fail_at,
                last_ok_at,
                updated_at
            ) VALUES (
                %(warmup_domain_id)s,
                %(domain)s,
                %(country_key)s,
                %(industry_raw)s,
                %(industry_category)s,
                %(industry_subcategory)s,
                %(warmup_interest)s,
                %(monthly_visits)s,
                %(yoy_traffic_change)s,
                %(source)s,
                %(imported_at)s,
                %(active)s,
                %(excluded_reason)s,
                %(fail_count)s,
                %(last_fail_reason)s,
                %(last_fail_at)s,
                %(last_ok_at)s,
                %(updated_at)s
            )
            ON CONFLICT (warmup_domain_id) DO UPDATE SET
                domain               = EXCLUDED.domain,
                country_key          = EXCLUDED.country_key,
                industry_raw         = EXCLUDED.industry_raw,
                industry_category    = EXCLUDED.industry_category,
                industry_subcategory = EXCLUDED.industry_subcategory,
                warmup_interest      = EXCLUDED.warmup_interest,
                monthly_visits       = EXCLUDED.monthly_visits,
                yoy_traffic_change   = EXCLUDED.yoy_traffic_change,
                source               = EXCLUDED.source,
                imported_at          = EXCLUDED.imported_at,
                active               = EXCLUDED.active,
                excluded_reason      = EXCLUDED.excluded_reason,
                fail_count           = EXCLUDED.fail_count,
                last_fail_reason     = EXCLUDED.last_fail_reason,
                last_fail_at         = EXCLUDED.last_fail_at,
                last_ok_at           = EXCLUDED.last_ok_at,
                updated_at           = EXCLUDED.updated_at
            WHERE public.warmup_domains.updated_at <= EXCLUDED.updated_at;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, row: dict) -> bool:
        """
        Marca processed=true en scraping.warmup_domains, solo si la fila no cambió
        desde que se leyó. Devuelve False si cambió (queda pendiente).
        """
        self.logger.debug(
            f"[warmup_domains_sync] Marcando processed=true para warmup_domain_id={row.get('warmup_domain_id')}"
        )

        query = """
            UPDATE public.warmup_domains
            SET processed = true,
                processed_at = NOW()
            WHERE warmup_domain_id = %(warmup_domain_id)s
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
                    f"[warmup_domains_sync] Fila salteada (warmup_domain_id={row.get('warmup_domain_id')}), "
                    f"queda pendiente para el próximo ciclo: {e}"
                )

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)

        if not rows:
            self.logger.info(
                "[warmup_domains_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[warmup_domains_sync] Procesando batch de {len(rows)} filas"
        )
        self.last_key = (rows[-1]["warmup_domain_id"],)

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
                f"[warmup_domains_sync] Batch OK ({len(rows)} filas procesadas"
                + (f", {changed} cambiaron durante la copia y quedan pendientes" if changed else "")
                + ")"
            )
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            # Problema de conexión: no tiene sentido reintentar fila por fila.
            raise
        except Exception as e:
            self.logger.warning(
                f"[warmup_domains_sync] Error en batch, rollback y reintento fila por fila: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()
            self.process_rows_one_by_one(rows)

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL warmup_domains_sync =====")
        self.connect()

        try:
            while self.process_batch():
                pass

            self.logger.info(
                f"ETL warmup_domains_sync finalizado. "
                f"Filas sincronizadas: {self.synced}, "
                f"cambiaron durante la copia: {self.changed}, "
                f"con error: {self.failed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL warmup_domains_sync =====")

# cookie_history_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class CookieHistorySync:
    """
    Sincroniza public.cookie_history (historial de cookies del cultivo)
    desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.cookie_history.
      - INSERT en prod.cookie_history con ON CONFLICT(cookie_event_id) DO NOTHING:
        la tabla es solo de inserts (cada fila es un evento que nunca cambia).
      - Marca processed = true, processed_at = NOW() en scraping.

    Es la tabla que más crece (cientos de filas por instancia y corrida), por
    eso inserta y marca por batch completo en vez de fila por fila. Si un
    batch falla, se reintenta fila por fila: la fila que falla se loguea y
    queda pendiente para el próximo ciclo, sin frenar al resto.
    """

    COLUMNS = [
        "cookie_event_id", "persona_instance_id", "run_id", "captured_at", "event",
        "warmup_domain_id", "visited_domain", "visited_url", "cookie_name", "cookie_value",
        "domain", "domain_clean", "path", "expires", "expires_at", "is_session",
        "http_only", "secure", "same_site", "is_third_party", "updated_at",
    ]

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("cookie_history_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None
        # Cursor de la corrida (ultimo id leido): avanza aunque haya filas
        # que fallen, que quedan pendientes para el próximo ciclo.
        self.last_id = 0
        self.synced = 0
        self.failed = 0

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (cookie_history_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (cookie_history_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (cookie_history_sync)")
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
        Filas pendientes en scraping.cookie_history (processed = false).
        """
        self.logger.info(
            f"[cookie_history_sync] Buscando filas processed=false (limite={limit})"
        )

        query = f"""
            SELECT {", ".join(self.COLUMNS)}
            FROM public.cookie_history
            WHERE processed = false
              AND cookie_event_id > %s
            ORDER BY cookie_event_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (self.last_id, limit))
            rows = cur.fetchall()

        self.logger.info(
            f"[cookie_history_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def insert_into_prod(self, rows: list[dict]):
        """
        INSERT del batch en prod.cookie_history. Las filas que ya estén
        (por un batch anterior que se cortó antes de marcar) se ignoran.
        """
        query = f"""
            INSERT INTO public.cookie_history ({", ".join(self.COLUMNS)})
            VALUES %s
            ON CONFLICT (cookie_event_id) DO NOTHING
        """

        values = [tuple(row[c] for c in self.COLUMNS) for row in rows]
        with self.prod_conn.cursor() as cur:
            execute_values(cur, query, values, page_size=500)

    def mark_as_processed(self, cookie_event_ids: list[int]):
        """
        Marca processed=true en scraping.cookie_history para el batch.
        """
        query = """
            UPDATE public.cookie_history
            SET processed = true,
                processed_at = NOW()
            WHERE cookie_event_id = ANY(%s)
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (cookie_event_ids,))

    # ---------- Lógica de batch ----------

    def sync_rows(self, rows: list[dict]):
        """Copia las filas a prod y las marca (sin commit)."""
        self.insert_into_prod(rows)
        self.mark_as_processed([row["cookie_event_id"] for row in rows])

    def process_rows_one_by_one(self, rows: list[dict]):
        """
        Reintento de un batch que falló: cada fila en su propia transacción.
        La que falla se loguea y queda pendiente (processed = false) para el
        próximo ciclo, sin frenar al resto.
        """
        for row in rows:
            try:
                self.sync_rows([row])
                self.prod_conn.commit()
                self.scraping_conn.commit()
                self.synced += 1
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                raise
            except Exception as e:
                self.prod_conn.rollback()
                self.scraping_conn.rollback()
                self.failed += 1
                self.logger.error(
                    f"[cookie_history_sync] Fila salteada (cookie_event_id={row.get('cookie_event_id')}), "
                    f"queda pendiente para el próximo ciclo: {e}"
                )

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)

        if not rows:
            self.logger.info(
                "[cookie_history_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[cookie_history_sync] Procesando batch de {len(rows)} filas"
        )
        self.last_id = rows[-1]["cookie_event_id"]

        try:
            self.sync_rows(rows)

            self.prod_conn.commit()
            self.scraping_conn.commit()
            self.synced += len(rows)

            self.logger.info(
                f"[cookie_history_sync] Batch OK ({len(rows)} filas procesadas)"
            )
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            # Problema de conexión: no tiene sentido reintentar fila por fila.
            raise
        except Exception as e:
            self.logger.warning(
                f"[cookie_history_sync] Error en batch, rollback y reintento fila por fila: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()
            self.process_rows_one_by_one(rows)

        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL cookie_history_sync =====")
        self.connect()

        try:
            while self.process_batch():
                pass

            self.logger.info(
                f"ETL cookie_history_sync finalizado. "
                f"Filas sincronizadas: {self.synced}, "
                f"con error: {self.failed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL cookie_history_sync =====")

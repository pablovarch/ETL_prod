# scan_orchestration_isp_dedicated.py
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log

# Keepalives TCP para que el SO detecte conexiones muertas y para que
# firewalls/NAT no corten sesiones largas por inactividad.
# Los valores del DSN tienen prioridad si ya los definen.
KEEPALIVE_OPTS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

COLUMNS = (
    "scan_id",
    "site",
    "proxy_country",
    "scanned",
    "crawler_method",
    "online_status",
    "offline_type",
    "redirect_url",
    "status_msg",
    "scanned_at",
)


class ScanOrchestrationIspDedicated:
    """
    ETL para sincronizar public.scan_orchestration_isp_dedicated
    desde la base de scraping hacia la base de producción.

    Solo se sincronizan filas ya escaneadas (scanned = true), con
    online_status no nulo y pendientes
    (processed = false). Requiere sql/scan_orchestration_isp_dedicated_scraping.sql
    aplicado en scraping: columnas processed/processed_at, índice parcial y
    trigger que vuelve a poner processed = false si el crawler modifica la fila.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("scan_orchestration_isp_dedicated_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos")

        try:
            self.scraping_conn = psycopg2.connect(**{**KEEPALIVE_OPTS, **SCRAPING_DB_DSN})
            self.prod_conn = psycopg2.connect(**{**KEEPALIVE_OPTS, **PROD_DB_DSN})
            self.logger.info("Conexiones establecidas correctamente")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos")
        for conn in (self.scraping_conn, self.prod_conn):
            try:
                if conn is not None and not conn.closed:
                    conn.close()
            except Exception as e:
                self.logger.exception(f"Error al cerrar conexion: {e}")

    def _safe_rollback(self, conn, name: str):
        """Rollback que no explota si la conexión ya se cayó."""
        if conn is None or conn.closed:
            self.logger.warning(f"Conexion {name} ya cerrada, se omite rollback")
            return
        try:
            conn.rollback()
        except Exception as e:
            self.logger.warning(f"Rollback en {name} fallo: {e}")

    # ---------- Operaciones de BD ----------

    def fetch_pending_rows(self, limit: int):
        """
        Obtiene filas escaneadas y pendientes desde scraping.

        FOR UPDATE bloquea las filas hasta el commit del batch: si el crawler
        intenta modificar una de ellas mientras tanto, espera, y su UPDATE
        (vía trigger) la vuelve a dejar pendiente. Así no se pierden cambios
        entre el SELECT y el mark_as_processed. SKIP LOCKED evita quedarse
        esperando filas que el crawler tiene bloqueadas en ese momento.
        """
        self.logger.info(f"Buscando filas pendientes en scraping (limite={limit})")

        query = f"""
            SELECT {", ".join(COLUMNS)}
            FROM public.scan_orchestration_isp_dedicated
            WHERE processed = false
              AND scanned = true
              AND online_status IS NOT NULL
            ORDER BY scan_id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(f"Filas pendientes encontradas: {len(rows)}")
        return rows

    def upsert_into_prod(self, rows: list[dict]):
        """
        Inserta o actualiza el batch completo en producción.
        Usa scan_id como PK compartida.
        """
        self.logger.debug(f"Upsert en prod de {len(rows)} filas")

        update_set = ",\n                ".join(
            f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "scan_id"
        )
        query = f"""
            INSERT INTO public.scan_orchestration_isp_dedicated ({", ".join(COLUMNS)})
            VALUES %s
            ON CONFLICT (scan_id) DO UPDATE SET
                {update_set}
        """

        values = [tuple(row[c] for c in COLUMNS) for row in rows]
        with self.prod_conn.cursor() as cur:
            execute_values(cur, query, values, page_size=500)

    def mark_as_processed(self, scan_ids: list[int]):
        """Marca el batch como procesado en scraping (un solo UPDATE)."""
        self.logger.debug(f"Marcando como procesadas {len(scan_ids)} filas")

        query = """
            UPDATE public.scan_orchestration_isp_dedicated
            SET processed = true,
                processed_at = NOW()
            WHERE scan_id = ANY(%s)
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (scan_ids,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        """
        Procesa un batch: lee pendientes, upsert en prod, marca processed
        en scraping. Devuelve cuántas filas procesó.
        """
        try:
            rows = self.fetch_pending_rows(self.batch_size)

            if not rows:
                self.logger.info("No hay filas pendientes para procesar")
                # Libera la transacción abierta por el SELECT.
                self.scraping_conn.rollback()
                return 0

            self.logger.info(f"Procesando batch de {len(rows)} filas")

            self.upsert_into_prod(rows)
            self.mark_as_processed([row["scan_id"] for row in rows])

            # Prod primero: si falla el commit de scraping, las filas se
            # reintentan y el upsert es idempotente.
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(f"Batch procesado correctamente ({len(rows)} filas)")
            return len(rows)
        except Exception as e:
            self.logger.exception(
                f"Error procesando batch, haciendo rollback en ambas bases: {e}"
            )
            self._safe_rollback(self.prod_conn, "prod")
            self._safe_rollback(self.scraping_conn, "scraping")
            # Relanzar: si no, run() vuelve a leer las mismas filas en loop.
            raise

    # ---------- Punto de entrada de la clase ----------

    def run(self):
        """
        Ejecuta el ETL completo: abre conexiones, procesa batches hasta
        quedarse sin pendientes y cierra conexiones.
        """
        self.logger.info("===== Inicio ETL scan_orchestration_isp_dedicated =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL finalizado. Total de filas procesadas: {total_processed}"
            )
        except psycopg2.Error as e:
            # Error de BD: se corta este ETL pero no el ciclo de main.py.
            # Lo pendiente se reintenta en el próximo ciclo.
            self.logger.error(
                f"ETL scan_orchestration_isp_dedicated abortado tras "
                f"{total_processed} filas: {e}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL scan_orchestration_isp_dedicated =====")

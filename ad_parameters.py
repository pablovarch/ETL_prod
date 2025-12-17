# ad_parameters.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class AdParameters:
    """
    ETL para sincronizar la tabla public.ad_parameters
    desde la base de scraping hacia la base de producción.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("ad_parameters_etl")
        self.batch_size = batch_size or BATCH_SIZE

        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        """Abre conexiones a ambas bases de datos."""
        self.logger.info("Iniciando conexiones a las bases de datos (ad_parameters)")

        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones establecidas correctamente (ad_parameters)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases de datos: {e}")
            raise

    def close(self):
        """Cierra las conexiones abiertas."""
        self.logger.info("Cerrando conexiones a las bases de datos (ad_parameters)")
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
        Obtiene filas pendientes (processed = false) desde la base de scraping.
        """
        self.logger.info(f"[ad_parameters] Buscando filas pendientes (limite={limit})")

        query = """
            SELECT
                parameter_id,
                "parameter",
                parameter_label,
                parameter_description,
                "type",
                ad_event_id,
                ad_bid_id
            FROM public.ad_parameters
            WHERE processed = false
            ORDER BY parameter_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(f"[ad_parameters] Filas pendientes encontradas: {len(rows)}")
        return rows

    def upsert_into_prod(self, row: dict):
        """
        Inserta o actualiza una fila en la tabla de producción.
        Usa parameter_id como PK compartida.
        """
        self.logger.debug(
            f"[ad_parameters] Upsert en prod para parameter_id={row.get('parameter_id')}"
        )

        query = """
            INSERT INTO public.ad_parameters (
                parameter_id,
                "parameter",
                parameter_label,
                parameter_description,
                "type",
                ad_event_id,
                ad_bid_id
            ) VALUES (
                %(parameter_id)s,
                %(parameter)s,
                %(parameter_label)s,
                %(parameter_description)s,
                %(type)s,
                %(ad_event_id)s,
                %(ad_bid_id)s
            )
            ON CONFLICT (parameter_id) DO UPDATE SET
                "parameter"             = EXCLUDED."parameter",
                parameter_label         = EXCLUDED.parameter_label,
                parameter_description   = EXCLUDED.parameter_description,
                "type"                  = EXCLUDED."type",
                ad_event_id             = EXCLUDED.ad_event_id,
                ad_bid_id               = EXCLUDED.ad_bid_id
        """

        # Mapeo porque en el dict viene "parameter" y "type" como claves
        params = {
            "parameter_id": row["parameter_id"],
            "parameter": row["parameter"],
            "parameter_label": row["parameter_label"],
            "parameter_description": row["parameter_description"],
            "type": row["type"],
            "ad_event_id": row["ad_event_id"],
            "ad_bid_id": row["ad_bid_id"],
        }

        with self.prod_conn.cursor() as cur:
            cur.execute(query, params)

    def mark_as_processed(self, parameter_id: str):
        """
        Marca una fila como procesada en la base de scraping.
        """
        self.logger.debug(
            f"[ad_parameters] Marcando como procesado parameter_id={parameter_id}"
        )

        query = """
            UPDATE public.ad_parameters
            SET processed = true,
                processed_at = NOW()
            WHERE parameter_id = %s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, (parameter_id,))

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        """
        Procesa un batch:
        - Lee filas pendientes.
        - Hace upsert en prod.
        - Marca como processed en scraping.

        Devuelve cuántas filas procesó.
        """
        rows = self.fetch_pending_rows(self.batch_size)

        if not rows:
            self.logger.info("[ad_parameters] No hay filas pendientes para procesar")
            return 0

        self.logger.info(f"[ad_parameters] Procesando batch de {len(rows)} filas")

        try:
            for row in rows:
                self.logger.debug(
                    f"[ad_parameters] Procesando parameter_id={row['parameter_id']} "
                    f"(ad_event_id={row['ad_event_id']}, ad_bid_id={row['ad_bid_id']})"
                )
                self.upsert_into_prod(row)
                self.mark_as_processed(row["parameter_id"])

            # Commit del batch en ambas bases
            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[ad_parameters] Batch procesado correctamente ({len(rows)} filas)"
            )
        except Exception as e:
            self.logger.exception(
                f"[ad_parameters] Error procesando batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()
            # opcional: raise si querés que el proceso externo falle
            # raise

        return len(rows)

    # ---------- Punto de entrada de la clase ----------

    def run(self):
        """
        Ejecuta el ETL completo:
        - Abre conexiones
        - Procesa batches hasta quedarse sin pendientes
        - Cierra conexiones
        """
        self.logger.info("===== Inicio ETL ad_parameters =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL ad_parameters finalizado. Total de filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL ad_parameters =====")

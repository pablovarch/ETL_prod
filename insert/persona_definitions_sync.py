# persona_definitions_sync.py
import psycopg2
from psycopg2.extras import RealDictCursor

from settings import SCRAPING_DB_DSN, PROD_DB_DSN, BATCH_SIZE
from logger import Log


class PersonaDefinitionsSync:
    """
    Sincroniza public.persona_definitions (plantillas de persona) desde scraping_db hacia prod_db.

    Lógica:
      - Lee filas con processed = false en scraping.persona_definitions.
      - UPSERT en prod.persona_definitions usando ON CONFLICT(persona_id); solo pisa la
        fila de prod si la que llega es igual o más nueva (updated_at).
      - Marca processed = true, processed_at = NOW() en scraping SOLO si la
        fila no cambió mientras se copiaba (mismo updated_at que se leyó).
        Si cambió, queda pendiente y se vuelve a copiar en el próximo batch.

    En scraping, un trigger vuelve a poner processed = false (y actualiza
    updated_at) cada vez que el cultivo modifica una fila.
    """

    def __init__(self, batch_size: int | None = None):
        self.logger = Log.get_logger("persona_definitions_sync_etl")
        self.batch_size = batch_size or BATCH_SIZE
        self.scraping_conn = None
        self.prod_conn = None

    # ---------- Conexiones ----------

    def connect(self):
        self.logger.info("Iniciando conexiones (persona_definitions_sync)")
        try:
            self.scraping_conn = psycopg2.connect(**SCRAPING_DB_DSN)
            self.prod_conn = psycopg2.connect(**PROD_DB_DSN)
            self.logger.info("Conexiones OK (persona_definitions_sync)")
        except Exception as e:
            self.logger.exception(f"Error al conectar a las bases: {e}")
            raise

    def close(self):
        self.logger.info("Cerrando conexiones (persona_definitions_sync)")
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
        Filas pendientes en scraping.persona_definitions (processed = false).
        """
        self.logger.info(
            f"[persona_definitions_sync] Buscando filas processed=false (limite={limit})"
        )

        query = """
            SELECT
                persona_id,
                persona,
                country_key,
                gender,
                interests,
                created_at,
                updated_at
            FROM public.persona_definitions
            WHERE processed = false
            ORDER BY persona_id
            LIMIT %s
        """

        with self.scraping_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()

        self.logger.info(
            f"[persona_definitions_sync] Filas pendientes encontradas: {len(rows)}"
        )
        return rows

    def upsert_into_prod(self, row: dict):
        """
        UPSERT en prod.persona_definitions usando persona_id como clave. No pisa una
        versión más nueva que ya esté en prod.
        """
        self.logger.debug(
            f"[persona_definitions_sync] Upsert prod para persona_id={row.get('persona_id')}"
        )

        query = """
            INSERT INTO public.persona_definitions (
                persona_id,
                persona,
                country_key,
                gender,
                interests,
                created_at,
                updated_at
            ) VALUES (
                %(persona_id)s,
                %(persona)s,
                %(country_key)s,
                %(gender)s,
                %(interests)s,
                %(created_at)s,
                %(updated_at)s
            )
            ON CONFLICT (persona_id) DO UPDATE SET
                persona     = EXCLUDED.persona,
                country_key = EXCLUDED.country_key,
                gender      = EXCLUDED.gender,
                interests   = EXCLUDED.interests,
                created_at  = EXCLUDED.created_at,
                updated_at  = EXCLUDED.updated_at
            WHERE public.persona_definitions.updated_at <= EXCLUDED.updated_at;
        """

        with self.prod_conn.cursor() as cur:
            cur.execute(query, row)

    def mark_as_processed(self, row: dict) -> bool:
        """
        Marca processed=true en scraping.persona_definitions, solo si la fila no cambió
        desde que se leyó. Devuelve False si cambió (queda pendiente).
        """
        self.logger.debug(
            f"[persona_definitions_sync] Marcando processed=true para persona_id={row.get('persona_id')}"
        )

        query = """
            UPDATE public.persona_definitions
            SET processed = true,
                processed_at = NOW()
            WHERE persona_id = %(persona_id)s
              AND updated_at = %(updated_at)s
        """

        with self.scraping_conn.cursor() as cur:
            cur.execute(query, row)
            return cur.rowcount == 1

    # ---------- Lógica de batch ----------

    def process_batch(self) -> int:
        rows = self.fetch_pending_rows(self.batch_size)

        if not rows:
            self.logger.info(
                "[persona_definitions_sync] No hay filas pendientes para procesar"
            )
            return 0

        self.logger.info(
            f"[persona_definitions_sync] Procesando batch de {len(rows)} filas"
        )

        try:
            changed = 0
            for row in rows:
                self.upsert_into_prod(row)
                if not self.mark_as_processed(row):
                    changed += 1

            self.prod_conn.commit()
            self.scraping_conn.commit()

            self.logger.info(
                f"[persona_definitions_sync] Batch OK ({len(rows)} filas procesadas"
                + (f", {changed} cambiaron durante la copia y quedan pendientes" if changed else "")
                + ")"
            )
        except Exception as e:
            self.logger.exception(
                f"[persona_definitions_sync] Error en batch, rollback en ambas bases: {e}"
            )
            self.prod_conn.rollback()
            self.scraping_conn.rollback()
            # Se corta esta tabla hasta el próximo ciclo: si se siguiera,
            # se volverían a leer las mismas filas pendientes indefinidamente.
            return 0

        if changed == len(rows):
            # Todas cambiaron mientras se copiaban (el cultivo las esta
            # actualizando ahora): se reintenta en el próximo ciclo.
            return 0
        return len(rows)

    # ---------- Punto de entrada ----------

    def run(self):
        self.logger.info("===== Inicio ETL persona_definitions_sync =====")
        self.connect()

        total_processed = 0
        try:
            while True:
                processed = self.process_batch()
                if processed == 0:
                    break
                total_processed += processed

            self.logger.info(
                f"ETL persona_definitions_sync finalizado. "
                f"Total filas procesadas: {total_processed}"
            )
        finally:
            self.close()
            self.logger.info("===== Fin ETL persona_definitions_sync =====")

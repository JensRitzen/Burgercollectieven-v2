class QualtricsPoller:
    def __init__(self, export_service, csv_handler, database, logger):
        self.export_service = export_service
        self.csv_handler = csv_handler
        self.database = database
        self.logger = logger

    def run_once(self):
        self.logger.info("Polling cycle gestart")

        zip_bytes = self.export_service.run_export()
        df = self.csv_handler.extract_dataframe(zip_bytes)

        before = self.database.count()

        for _, row in df.iterrows():
            response_id = str(row.get("ResponseId"))
            if response_id.startswith("R_"):
                self.database.upsert(response_id, row.to_json())

        after = self.database.count()
        self.logger.info(f"DB records: {after} (Δ {after - before})")

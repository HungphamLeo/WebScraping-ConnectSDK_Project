

class ExtractorFactory:
    @staticmethod
    def get_extractor(source_type):
        if source_type == "web":
            return WebExtractor()
        elif source_type == "file":
            return FileExtractor()

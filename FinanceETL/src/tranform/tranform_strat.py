class TransformStrategy:
    def process(self, data):
        raise NotImplementedError("Strategy method must be implemented")

class CleanTransform(TransformStrategy):
    def process(self, data):
        # Làm sạch dữ liệu, bỏ null hoặc xử lý lỗi định dạng
        return cleaned_data

class AggregateTransform(TransformStrategy):
    def process(self, data):
        # Tổng hợp dữ liệu theo nhóm hoặc cột
        return aggregated_data

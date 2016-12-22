from . update_db import WeightsUpdateDB
from . completed_db import WeightsCompletedDB


class WeightsDB():
    MAX_RANGE = 250
    MIN_RECORDS = 3
    update_db = WeightsUpdateDB()
    completed_db = WeightsCompletedDB()

    def copy_update_item_to_completed(self, stock_id, weight):
        self.completed_db.query(
            'INSERT INTO {} (stock_id, weight) VALUES ("{}", {})'.format(
                self.completed_db.table, stock_id, str(weight)))

    def complete_item(self, stock_id, weight):
        self.copy_update_item_to_completed(stock_id, weight)
        self.remove_from_update_db(stock_id)

    def get_completed_weight(self, stock_id):
        return self.completed_db.get_weight(stock_id)

    def is_valid_weight_range(self, weight_list):
        record_count = len(weight_list)
        record_range = max(weight_list) - min(weight_list)
        if record_count >= self.MIN_RECORDS and \
                record_range <= self.MAX_RANGE:
                return True
        return False

    def remove_from_update_db(self, stock_id):
        self.update_db.remove_item(stock_id)

    def update(self):
        update_data = self.update_db.get_all()
        updated_item_count = 0
        for stock_id, weight_list in update_data.items():
            if self.is_valid_weight_range(weight_list):
                weight = self.update_db.get_average_weight(weight_list)
                self.complete_item(stock_id, weight)
                updated_item_count += 1
        return updated_item_count

    def get_invalid_weight_lists(self):
        update_data = self.update_db.get_all()
        invalid_items = {}
        for stock_id, weight_list in update_data.items():
            record_range = max(weight_list) - min(weight_list)
            if len(weight_list) >= 3 and record_range > self.MAX_RANGE:
                invalid_items[stock_id] = weight_list
        return invalid_items

import json
import statistics

from linnworks_db import LinnworksDB


class WeightsUpdateDB(LinnworksDB):
    name = 'Update Weights'
    table = 'weights'
    stock_id_field = 'stock_id'
    weights_field = 'weights_string'

    def add_weight(self, stock_id, weight):
        if self.stock_id_exists(stock_id):
            weight_list = self.get_weight_list(stock_id)
        else:
            weight_list = []
        weight_list.append(weight)
        weight_string = json.dumps(weight_list)
        query = "INSERT INTO {} ({}, {}) VALUES ".format(
            self.table, self.stock_id_field, self.weights_field)
        query += "('{}', '{}') ON DUPLICATE KEY UPDATE {}=VALUES({})".format(
            stock_id, weight_string, self.weights_field, self.weights_field)
        self.query(query)

    def get_all(self):
        query = "SELECT `{}`, `{}` FROM {}".format(
            self.stock_id_field, self.weights_field, self.table)
        query_result = self.query(query)
        result = {record[0]: json.loads(record[1]) for record in query_result}
        return result

    def get_stock_ids(self):
        query = "SELECT {} FROM {}".format(
            self.stock_id_field, self.table)
        result = self.query(query)
        stock_ids = [record[0] for record in result]
        return stock_ids

    def get_weight_list(self, stock_id):
        query = "SELECT {} FROM {} WHERE {}='{}'".format(
            self.weights_field, self.table, self.stock_id_field, stock_id)
        result = self.query(query)
        return json.loads(result[0][0])

    def stock_id_exists(self, stock_id):
        stock_ids = self.get_stock_ids()
        if stock_id in stock_ids:
            return True
        return False

    def get_weight(self, stock_id):
        weight_list = self.get_weight_list(stock_id)
        return self.get_average_weight(weight_list)

    def get_average_weight(self, weight_list):
        return statistics.mean(weight_list)

    def remove_weight(self, stock_id, weight):
        item_weight_list = self.get_weight_list(stock_id)
        updated_weight_list = [x for x in item_weight_list if x != weight]
        self.replace_weight_list(stock_id, updated_weight_list)

    def replace_weight_list(self, stock_id, weight_list):
        weight_list_json = json.dumps(weight_list)
        query = "UPDATE {} SET `{}`='{}' WHERE `{}`='{}'".format(
            self.table, self.weights_field, weight_list_json,
            self.stock_id_field, stock_id)
        self.query(query)

    def remove_item(self, stock_id):
        query = 'DELETE FROM {} WHERE stock_id="{}"'.format(
            self.table, stock_id)
        self.query(query)

    def get_weight_range(self, stock_id):
        weight_list = self.get_weight_list(stock_id)
        return max(weight_list) - min(weight_list)

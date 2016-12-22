from linnworks_db import LinnworksDB


class WeightsCompletedDB(LinnworksDB):
    name = 'Completed Weights'
    table = 'weights_completed'
    stock_id_field = 'stock_id'
    weight_field = 'weight'

    def get_stock_ids(self):
        query = "SELECT {} FROM {}".format(
            self.stock_id_field, self.table)
        result = self.query(query)
        stock_ids = [record[0] for record in result]
        return stock_ids

    def get_weight(self, stock_id):
        query = "SELECT {} FROM {} WHERE `{}`='{}'".format(
            self.weight_field, self.table, self.stock_id_field, stock_id)
        result = self.query(query)
        try:
            return int(result[0][0])
        except IndexError:
            raise ValueError('No completed weight for item.')

import os

class Pipeline:
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT')

    def ingest(self):
        if self.environment == 'production':
            print('Ingesting data for production...')
        else:
            print('Ingesting data for development...')

    def transform_silver(self):
        if self.environment == 'production':
            print('Transforming data to silver for production...')
        else:
            print('Transforming data to silver for development...')

    def transform_gold(self):
        if self.environment == 'production':
            print('Transforming data to gold for production...')
        else:
            print('Transforming data to gold for development...')

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.ingest()
    pipeline.transform_silver()
    pipeline.transform_gold()
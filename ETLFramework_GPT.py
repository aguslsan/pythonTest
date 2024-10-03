import yaml


class ETLFramework_GPT:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config

    def execute_transformations(self, data):
        for transformation in self.config['transformations']:
            name = transformation['name']
            parameters = {param['name']: param['value'] for param in transformation['parameters']}
            data = self.apply_transformation(name, data, **parameters)
        return data

    def apply_transformation(self, name, data, **kwargs):
        if name == 'uppercase':
            column_name = kwargs['column_name']
            data = [x.upper() if idx == column_name else x for idx, x in enumerate(data)]
        elif name == 'add_prefix':
            column_name = kwargs['column_name']
            prefix = kwargs['prefix']
            column_index = None
            for idx, col in enumerate(data):
                if col == column_name:
                    column_index = idx
                    break
            if column_index is not None:
                data[column_index] = [prefix + x for x in data[column_index]]
        return data

# Example usage
if __name__ == "__main__":
    config_file = 'mappings.yaml'
    etl_framework = ETLFramework_GPT(config_file)

    # Example data (replace with your actual data source)
    data = [
        ['hello', 'world'],
        ['John', 'Alice']
    ]

    transformed_data = etl_framework.execute_transformations(data)
    print(transformed_data)

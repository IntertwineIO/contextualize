from unittest.mock import MagicMock

from extraction.operation import ExtractionOperation


class ReferencePath:

    def __init__(self, path, value):
        path_component_names = path.split('.')
        path_length = len(path_component_names)
        if not path_length:
            raise ValueError(f'Invalid path: {path}')
        self.name = path_component_names[0]
        if path_length == 1:
            setattr(self, self.name, value)
        else:
            sub_path = '.'.join(path_component_names[1:])
            cls = self.__class__
            sub_component = cls(sub_path, value)
            setattr(self, self.name, sub_component)

    def __repr__(self):
        path_component_names = []
        value = self
        while isinstance(value, type(self)):
            name = value.name
            path_component_names.append(name)
            value = getattr(value, name)

        path = '.'.join(path_component_names)
        return f"ReferencePath('{path}', {value})"


CONTENT_MAP = {
    'alpha': 'dog',
    'beta': ReferencePath('max', 1975),
    'gamma': ReferencePath('ray.burst', '110328A'),
    'delta': ReferencePath('delta.delta', 'ΔΔΔ')
}


class ExtractionOperationBuilder:

    def __init__(self, scope=None, is_multiple=None,
                 find_method=None, find_args=None,
                 wait_method=None, wait_args=None,
                 wait=None, click=None,
                 extract_method=None, extract_args=None,
                 get_method=None, get_args=None,
                 parse_method=None, parse_args=None,
                 format_method=None, format_args=None,
                 transform_method=None, transform_args=None,
                 field=None, source=None, extractor=None):

        self.scope = scope
        self.is_multiple = is_multiple
        self.find_method = find_method
        self.find_args = find_args
        self.wait_method = wait_method
        self.wait_args = wait_args
        self.wait = wait
        self.click = click
        self.extract_method = extract_method
        self.extract_args = extract_args
        self.get_method = get_method
        self.get_args = get_args
        self.parse_method = parse_method
        self.parse_args = parse_args
        self.format_method = format_method
        self.format_args = format_args
        self.transform_method = transform_method
        self.transform_args = transform_args

        self.field = field or 'test_field'
        self.source = source or 'test_source'
        self.extractor = extractor or self._build_mock_extractor()

    def _build_mock_extractor(self):
        mock_extractor = MagicMock()
        mock_extractor.content_map = CONTENT_MAP
        return mock_extractor

    def build(self):
        return ExtractionOperation(**self.to_dict())

    def to_dict(self):
        return self.__dict__

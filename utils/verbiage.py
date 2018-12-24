import ast
from enum import Enum
from numbers import Number
from string import Template


class ClearTemplate(Template):
    """String Template with improved str, repr and comparison support"""
    def __str__(self):
        return self.template

    def __repr__(self):
        return f'{self.__class__.__qualname__}({self.template!r})'

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.template == other.template
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return self.template != other.template
        return NotImplemented


class Plurality:
    """
    Plurality

    Obtain singular/plural forms based on a number.

    Arguments may include a number, a single/plural forms string, and/or
    template strings. All arguments are optional and may be specified in
    any order. Plurality instances are callable, accept all arguments,
    and return new Plurality instances to enable chaining.

    Numbers may be any numeric type.

    Singular/plural forms are specified by one of these string formats:
        '{base}/{singular_suffix}/{plural_suffix}', e.g. 'cact/us/i'
        '{base}/{plural_suffix}', e.g. 'tree/s'
        '{base}', e.g. 'deer'
        Credit for this format: https://stackoverflow.com/a/27642538

    Templates are specified as follows, with multiple delimited by ';':
        '{n}={template_string}', e.g. '1=$n $thing;n=$n $things'
        where '{n}' is the number for which the template should be used
            or 'n' to specify the default template
        and where '{template_string}' may include these tokens:
            '$n' for the number
            '$thing' for the singular form
            '$things' for the plural form

    Usage:

    >>> from utils.verbiage import Plurality

    >>> f"We have {Plurality(0, 'octop/us/odes')}."
    'We have 0 octopodes.'
    >>> f"We have {Plurality(1, 'octop/us/odes')}."
    'We have 1 octopus.'
    >>> f"We have {Plurality(2, 'octop/us/odes')}."
    'We have 2 octopodes.'

    >>> oxen = Plurality('ox/en')
    >>> oxen.template_formatter
    '1=$n $thing;n=$n $things'
    >>> f"We have {oxen(0)}."
    'We have 0 oxen.'
    >>> f"We have {oxen(1)}."
    'We have 1 ox.'
    >>> f"We have {oxen(2)}."
    'We have 2 oxen.'

    >>> cows = Plurality('/cow/kine', '0=no $things', '1=a $thing')
    >>> cows.template_formatter
    '0=no $things;1=a $thing;n=$n $things'
    >>> f"We have {cows(0)}."
    'We have no kine.'
    >>> f"We have {cows(1)}."
    'We have a cow.'
    >>> f"We have {cows(2)}."
    'We have 2 kine.'

    >>> 'We have {:0=no $things;0.5=half a $thing}.'.format(Plurality(0, 'g/oose/eese'))
    'We have no geese.'
    >>> 'We have {:g/oose/eese;0=no $things;0.5=half a $thing}.'.format(Plurality(0.5))
    'We have half a goose.'
    >>> 'We have {:4;g/oose/eese;0=no $things;0.5=half a $thing}.'.format(Plurality())
    'We have 4 geese.'

    >>> data = {'tree': 1, 'bush': 2, 'flower': 3, 'cactus': 0}
    >>> s = "We have {tree:tree/s}, {bush:bush/es}, {flower:flower/s}, and {cactus:cact/us/i}."
    >>> s.format_map({k: Plurality(v) for k, v in data.items()})
    'We have 1 tree, 2 bushes, 3 flowers, and 0 cacti.'
    >>> vague = Plurality('0=no $things;1=a $thing;2=a couple $things;n=some $things')
    >>> s.format_map({k: vague(v) for k, v in data.items()})
    'We have a tree, a couple bushes, some flowers, and no cacti.'
    """
    FORM_DELIMITER = '/'
    FORMATTER_DELIMITER = ';'
    TEMPLATE_ASSIGNER = '='

    NUMBER_TOKEN = 'n'
    SINGULAR_TOKEN = 'thing'
    PLURAL_TOKEN = 'things'

    TEMPLATE_CLASS = ClearTemplate

    TEMPLATE_DEFAULTS = {
        1: TEMPLATE_CLASS(f'${NUMBER_TOKEN} ${SINGULAR_TOKEN}'),  # '1=1 $thing'
        NUMBER_TOKEN: TEMPLATE_CLASS(f'${NUMBER_TOKEN} ${PLURAL_TOKEN}')  # 'n=$n $things'
    }

    class Formatter(Enum):
        NUMBER = 'number_formatter'
        FORM = 'form_formatter'
        TEMPLATE = 'template_formatter'

    class CustomFormatter(Enum):
        NUMBER = 'number_formatter'
        FORM = 'form_formatter'
        TEMPLATE = 'custom_template_formatter'

    def __init__(self, *args):
        super().__init__()
        self.number = None
        self.singular = None
        self.plural = None
        self.templates = self.TEMPLATE_DEFAULTS
        self._configure_from_args(*args)

    def clone(self, deep=False):
        inst = self.__class__()
        inst.number, inst.singular, inst.plural = self.number, self.singular, self.plural
        inst.templates = self.templates.copy() if deep else self.templates
        return inst

    def clone_with(self, *args, deep=False, override=True):
        inst = self.clone(deep=deep)
        inst._configure_from_args(*args, override=override)
        return inst

    def __call__(self, *args, deep=False):
        return self.clone_with(*args, deep=deep, override=False)

    def __str__(self):
        kwargs = {}

        if self.number is not None:
            kwargs[self.NUMBER_TOKEN] = self.number
        if self.singular is not None:
            kwargs[self.SINGULAR_TOKEN] = self.singular
        if self.plural is not None:
            kwargs[self.PLURAL_TOKEN] = self.plural

        template = self.templates.get(self.number, self.templates[self.NUMBER_TOKEN])
        return template.safe_substitute(**kwargs)

    def __repr__(self):
        class_name = self.__class__.__qualname__
        number = self.number if self.number is not None else ''
        forms = f'{self.form_formatter!r}' if self.form_formatter else ''
        custom_template_formatter = self.custom_template_formatter
        templates = (f'{custom_template_formatter!r}' if custom_template_formatter else '')
        delimiter1 = ', ' if number != '' and (forms or templates) else ''
        delimiter2 = ', ' if forms and templates else ''
        return f'{class_name}({number}{delimiter1}{forms}{delimiter2}{templates})'

    def __add__(self, other):
        return str(self) + other

    def __radd__(self, other):
        return other + str(self)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.number == other.number and
                    self.singular == other.singular and
                    self.plural == other.plural and
                    self.templates == other.templates)
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not (self == other)
        return NotImplemented

    def __format__(self, formatter):
        if not formatter:
            return str(self)
        substrings = formatter.split(self.FORMATTER_DELIMITER)
        args = (self._deformat(substring) for substring in substrings)
        return str(self(*args))

    @property
    def is_complete(self):
        return bool((self.number is not None) and self.singular and self.plural)

    @property
    def formatter(self):
        return self.FORMATTER_DELIMITER.join(self.formatters)

    @property
    def formatters(self):
        return self._build_formatters(self.Formatter)

    @property
    def custom_formatter(self):
        return self.FORMATTER_DELIMITER.join(self.custom_formatters)

    @property
    def custom_formatters(self):
        return self._build_formatters(self.CustomFormatter)

    def _build_formatters(self, formatter_enum):
        formatters = []
        formatter_names = (formatter_option.value for formatter_option in formatter_enum)
        for formatter_name in formatter_names:
            formatter = getattr(self, formatter_name)
            if formatter:
                formatters.append(formatter)
        return formatters

    @property
    def number_formatter(self):
        return str(self.number) if self.number is not None else None

    @property
    def form_formatter(self):
        singular, plural = self.singular, self.plural
        if not singular or not plural:
            return
        if singular == plural:
            return singular
        if plural.startswith(singular):
            plural_suffix = plural[len(singular)-len(plural):]
            return f'{singular}{self.FORM_DELIMITER}{plural_suffix}'
        for i in range(0, len(singular) - 1):
            if singular[i] != plural[i]:
                break
        base = singular[:i]
        singular_suffix = singular[i:]
        plural_suffix = plural[i:]
        return f'{base}{self.FORM_DELIMITER}{singular_suffix}{self.FORM_DELIMITER}{plural_suffix}'

    @property
    def template_formatter(self):
        return self.FORMATTER_DELIMITER.join(self.template_formatters)

    @property
    def template_formatters(self):
        return sorted(f'{k}{self.TEMPLATE_ASSIGNER}{v.template}' for k, v in self.templates.items())

    @property
    def custom_template_formatter(self):
        return self.FORMATTER_DELIMITER.join(self.custom_template_formatters)

    @property
    def custom_template_formatters(self):
        return sorted(f'{k}{self.TEMPLATE_ASSIGNER}{v.template}'
                      for k, v in self.custom_template_items)

    @property
    def custom_templates(self):
        return dict(self.custom_template_items)

    @property
    def custom_template_items(self):
        return ((k, v) for k, v in self.templates.items() if not self.is_default_template(k, v))

    def is_default_template(self, key, template=None):
        template = template or self.templates[key]
        default_template = self.TEMPLATE_DEFAULTS.get(key)
        if template and default_template:
            return template.template == default_template.template
        return template == default_template

    @classmethod
    def is_template_formatter(cls, formatter):
        return cls.TEMPLATE_ASSIGNER in formatter

    def _deformat(self, formatter):
        if self.TEMPLATE_ASSIGNER in formatter:
            return formatter
        if self.FORM_DELIMITER in formatter:
            return formatter
        try:
            return ast.literal_eval(formatter)
        except ValueError:
            return formatter

    def _configure_from_args(self, *args, override=False):
        templates_copied = number_configured = forms_configured = False
        for arg in args:
            if isinstance(arg, Number):
                self._configure_number(arg, number_configured, override)
                number_configured = True
            elif isinstance(arg, str):
                if self.is_template_formatter(arg):
                    if not templates_copied:
                        self.templates = self.templates.copy()
                        templates_copied = True
                    self._configure_templates(arg)
                else:
                    self._configure_forms(arg, forms_configured, override)
                    forms_configured = True
            else:
                raise TypeError('Arguments must be numbers or strings')

    def _configure_number(self, number, is_configured=False, override=False):
        if is_configured or (not override and self.number is not None):
            raise ValueError('Number has already been configured')
        self.number = number

    def _configure_templates(self, formatter):
        if formatter:
            for sub_formatter in formatter.split(self.FORMATTER_DELIMITER):
                try:
                    key, value = sub_formatter.split(self.TEMPLATE_ASSIGNER)
                except ValueError:
                    raise ValueError(f'Invalid template formatter: {sub_formatter!r}')
                if key != self.NUMBER_TOKEN:
                    key = ast.literal_eval(key)
                self.templates[key] = self.TEMPLATE_CLASS(value)

    def _configure_forms(self, formatter, is_configured=False, override=False):
        singular, plural = self._derive_forms(formatter)
        if is_configured or (not override and (self.singular or self.plural)):
            raise ValueError('Singular/plural forms have already been configured')
        self.singular, self.plural = singular, plural

    def _derive_forms(self, formatter):
        base, _, suffixes = formatter.partition(self.FORM_DELIMITER)
        singular_suffix, _, plural_suffix = suffixes.rpartition(self.FORM_DELIMITER)
        singular = base + singular_suffix
        plural = base + plural_suffix
        return singular, plural

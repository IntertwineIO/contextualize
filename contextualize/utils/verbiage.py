import ast
import contextlib
import json
import os
import string
from collections import OrderedDict
from enum import Enum
from numbers import Number
from string import Template

import nltk


class PronunciationGuide:

    CORPUS_NAME_DEFAULT = 'cmudict'

    def __init__(self, corpus_name=CORPUS_NAME_DEFAULT):
        """Initialize instance with corpus name, by default cmudict"""
        self.corpus_name = corpus_name
        self._dictionary = None

    @property
    def dictionary(self):
        """Return dictionary, provisioning or updating it as needed"""
        if self._dictionary is None:
            try:
                self.provision_dictionary()
            except LookupError:
                self.refresh_dictionary()
        return self._dictionary

    def refresh_dictionary(self, corpus_name=None):
        """Refresh dictionary corpus by downloading & provisioning it"""
        self.download_dictionary(corpus_name)
        self.provision_dictionary(corpus_name)

    def download_dictionary(self, corpus_name=None):
        """Download dictionary corpus"""
        corpus_name = corpus_name or self.corpus_name
        nltk.download(corpus_name)

    def provision_dictionary(self, corpus_name=None):
        """Provision dictionary from corpus and set on instance"""
        corpus_name = corpus_name or self.corpus_name
        corpus = getattr(nltk.corpus, corpus_name)
        self._dictionary = corpus.dict()
        self.corpus_name = corpus_name

    def deprovision_dictionary(self):
        """Deprovision dictionary by removing reference"""
        self._dictionary = None


class JsonFileMixin:
    """JSON File Mixin for basic read/write/removal of JSON files"""
    DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
    DIRECTORY_NAME = '.tmp'

    @classmethod
    def _read_file(cls, file_name):
        """Read JSON from file with given name and marshal to object"""
        file_path = os.path.join(cls.DIRECTORY_PATH, cls.DIRECTORY_NAME, file_name)
        with open(file_path) as file:
            content_json = file.read()
        return json.loads(content_json)

    @classmethod
    def _write_file(cls, file_name, content):
        """Write jsonable content to file with given file name"""
        directory_path = os.path.join(cls.DIRECTORY_PATH, cls.DIRECTORY_NAME)
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, file_name)
        content_json = json.dumps(content, indent=4)
        with open(file_path, 'w') as file:
            file.write(content_json)

    @classmethod
    def _remove_file(cls, file_name):
        """Remove file with given name, if any"""
        file_path = os.path.join(cls.DIRECTORY_PATH, cls.DIRECTORY_NAME, file_name)
        with contextlib.suppress(FileNotFoundError):
            os.remove(file_path)


class FirstSoundGuide(JsonFileMixin, PronunciationGuide):
    """
    First Sound Guide

    Utility for determining if first sound of text is a vowel sound.

    Credit for nltk-based approach to determine vowel sounds:
    https://stackoverflow.com/a/20337527/4182210

    Special cases are cached in files to avoid keeping entire dictionary
    of pronunciations (currently over 123k words) in memory.
    """
    VOWELS = set('aeiou')
    CONSONANTS = set(string.ascii_lowercase) - VOWELS

    VOWEL_SOUNDING_CONSONANT_FILE = 'vowel_sounding_consonant_led_words.json'
    CONSONANT_SOUNDING_VOWEL_FILE = 'consonant_sounding_vowel_led_words.json'

    def __init__(self):
        super().__init__()
        self._consonant_sounding_vowel_led_words = None
        self._vowel_sounding_consonant_led_words = None

    def led_by_vowel_sound(self, text):
        """Determine if given text is led by a vowel sound"""
        text = text.strip()
        space_index = text.find(' ')
        first_word = text[:space_index] if space_index > 0 else text
        cleansed = first_word.lstrip('$(`"\'').rstrip(').!?:;-`"\'').lower()

        # Handle acronyms/initials
        period_index = cleansed.find('.')
        if period_index > 0:
            cleansed = cleansed[:period_index]

        # Handle hyphenated
        hyphen_index = cleansed.find('-')
        if hyphen_index > 0:
            cleansed = cleansed[:hyphen_index]

        # Handle words starting with vowels
        if cleansed[0] in self.VOWELS:
            return cleansed not in self.consonant_sounding_vowel_led_words

        # Handle words starting with consonants
        elif cleansed[0] in self.CONSONANTS:
            return cleansed in self.vowel_sounding_consonant_led_words

        # Handle numeric
        try:
            # TODO: handle measures: $10k, $40M, $8B, 2.1T 10cc, 08:30am, 80%
            cleansed = cleansed.replace(',', '')
            ast.literal_eval(cleansed)
            return cleansed[0] == '8' or (cleansed[:2] in {'11', '18'} and
                                          (len(cleansed) % 3 == 2 or len(cleansed) == 4))
        except (SyntaxError, ValueError):
            return

    def first_sound(self, word):
        """Return first phoneme of a dictionary word"""
        try:
            pronunciations = self.dictionary[word]
        except KeyError:
            return None
        else:
            primary_pronunciation = pronunciations[0]
            return primary_pronunciation[0]

    def first_sound_is_vowel(self, word):
        """Determine if dictionary word is led by a vowel sound"""
        first_phoneme = self.first_sound(word)
        return self.phoneme_is_vowel(first_phoneme) if first_phoneme else first_phoneme

    @staticmethod
    def phoneme_is_vowel(phoneme):
        """Determine if given ARPAbet phoneme is a vowel"""
        # vowels end with a lexical stress marker:
        # http://www.speech.cs.cmu.edu/cgi-bin/cmudict
        return phoneme[-1].isdigit()

    @property
    def consonant_sounding_vowel_led_words(self):
        """Return dict of words led by vowels that sound like consonants"""
        if self._consonant_sounding_vowel_led_words is None:
            try:
                pronunciations = self._read_file(self.CONSONANT_SOUNDING_VOWEL_FILE)
            except FileNotFoundError:
                pronunciations = OrderedDict(
                    (w, v) for w, v in self.dictionary.items()
                    if w[0] in self.VOWELS and not self.first_sound_is_vowel(w))
            if pronunciations:
                self._consonant_sounding_vowel_led_words = pronunciations
                self._write_file(self.CONSONANT_SOUNDING_VOWEL_FILE, pronunciations)
        return self._consonant_sounding_vowel_led_words

    @property
    def vowel_sounding_consonant_led_words(self):
        """Return dict of words led by consonants that sound like vowels"""
        if self._vowel_sounding_consonant_led_words is None:
            try:
                pronunciations = self._read_file(self.VOWEL_SOUNDING_CONSONANT_FILE)
            except FileNotFoundError:
                pronunciations = OrderedDict(
                    (w, v) for w, v in self.dictionary.items()
                    if w[0] in self.CONSONANTS and self.first_sound_is_vowel(w))
            if pronunciations:
                self._vowel_sounding_consonant_led_words = pronunciations
                self._write_file(self.VOWEL_SOUNDING_CONSONANT_FILE, pronunciations)
        return self._vowel_sounding_consonant_led_words

    def refresh_dictionary(self, corpus_name=None):
        """Refresh dictionary per given corpus name and clear cache"""
        super().refresh_dictionary(corpus_name)
        self.clear_cache()

    def clear_cache(self):
        """Clear file-based cache and in-memory cache reference"""
        self.remove_cache_files()
        self._consonant_sounding_vowel_led_words = None
        self._vowel_sounding_consonant_led_words = None

    @classmethod
    def remove_cache_files(cls):
        """Remove cache files"""
        cls._remove_file(cls.CONSONANT_SOUNDING_VOWEL_FILE)
        cls._remove_file(cls.VOWEL_SOUNDING_CONSONANT_FILE)


FIRST_SOUND_GUIDE = FirstSoundGuide()


def a(text):
    """Prepend given text with proper indefinite article (a/an)"""
    return f'an {text}' if FIRST_SOUND_GUIDE.led_by_vowel_sound(text) else f'a {text}'

# Allow `an()` to be used interchangeably for improved readability
an = a


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

    Obtain singular/plural form based on a number.

    Arguments may include a number, a single/plural form string, and/or
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
            '$a' for the proper indefinite article (a/an)
            '$n' for the number
            '$thing' for the singular form
            '$things' for the plural form

    Usage:

    >>> f"We have {Plurality(0, 'g/oose/eese')}."
    'We have 0 geese.'
    >>> f"We have {Plurality(1, 'g/oose/eese')}."
    'We have 1 goose.'
    >>> f"We have {Plurality(2, 'g/oose/eese')}."
    'We have 2 geese.'

    >>> oxen = Plurality('ox/en')
    >>> oxen.template_formatter
    '1=$n $thing;n=$n $things'
    >>> f"We have {oxen(0)}."
    'We have 0 oxen.'
    >>> f"We have {oxen(1)}."
    'We have 1 ox.'
    >>> f"We have {oxen(2)}."
    'We have 2 oxen.'

    >>> cows = Plurality('/cow/kine', '0=no $things', '1=$a $thing')
    >>> cows.template_formatter
    '0=no $things;1=a $thing;n=$n $things'
    >>> f"We have {cows(0)}."
    'We have no kine.'
    >>> f"We have {cows(1)}."
    'We have a cow.'
    >>> f"We have {cows(2)}."
    'We have 2 kine.'

    >>> 'We have {:0=no $things;0.5=half $a $thing}.'.format(Plurality(0, 'octop/us/odes'))
    'We have no octopodes.'
    >>> 'We have {:octop/us/odes;0=no $things;0.5=half $a $thing}.'.format(Plurality(0.5))
    'We have half an octopus.'
    >>> 'We have {:4;octop/us/odes;0=no $things;0.5=half $a $thing}.'.format(Plurality())
    'We have 4 octopodes.'

    >>> data = {'herb': 1, 'bush': 2, 'flower': 3, 'cactus': 0}
    >>> s = "We have {herb:herb/s}, {bush:bush/es}, {flower:flower/s}, and {cactus:cact/us/i}."
    >>> s.format_map({k: Plurality(v) for k, v in data.items()})
    'We have 1 herb, 2 bushes, 3 flowers, and 0 cacti.'
    >>> vague = Plurality('0=no $things;1=$a $thing;2=a couple $things;n=some $things')
    >>> s.format_map({k: vague(v) for k, v in data.items()})
    'We have an herb, a couple bushes, some flowers, and no cacti.'
    """
    FORM_DELIMITER = '/'
    FORMATTER_DELIMITER = ';'
    TEMPLATE_ASSIGNER = '='

    ARTICLE_TOKEN = 'a'
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
        self.template_map = self.TEMPLATE_DEFAULTS
        self._configure_from_args(*args)

    def clone(self, deep=False):
        """Clone instance with shared templates unless deep is True"""
        inst = self.__class__()
        inst.number, inst.singular, inst.plural = self.number, self.singular, self.plural
        inst.template_map = self.template_map.copy() if deep else self.template_map
        return inst

    def clone_with(self, *args, deep=False, override=True):
        """
        Clone instance with given args

        I/O:
        args:           Number, forms, and/or template
        deep=False:     By default, templates are only copied if args
                        include templates, else templates are shared.
                        If True, templates are always copied.
        override=True:  If True (default), args may override existing
                        values. If False, raise on attempted overrides.
        """
        inst = self.clone(deep=deep)
        inst._configure_from_args(*args, override=override)
        return inst

    def __call__(self, *args, deep=False, override=False):
        """Shorthand for clone_with(), but defaulting override to False"""
        return self.clone_with(*args, deep=deep, override=override)

    def __repr__(self):
        class_name = self.__class__.__qualname__
        number = self.number if self.number is not None else ''
        forms = f'{self.form_formatter!r}' if self.form_formatter else ''
        custom_template_formatter = self.custom_template_formatter
        templates = (f'{custom_template_formatter!r}' if custom_template_formatter else '')
        delimiter1 = ', ' if number != '' and (forms or templates) else ''
        delimiter2 = ', ' if forms and templates else ''
        return f'{class_name}({number}{delimiter1}{forms}{delimiter2}{templates})'

    def __str__(self):
        """Render the number-appropriate template to a string"""
        kwargs = {}

        if self.number is not None:
            kwargs[self.NUMBER_TOKEN] = self.number
        if self.singular is not None:
            kwargs[self.SINGULAR_TOKEN] = self.singular
        if self.plural is not None:
            kwargs[self.PLURAL_TOKEN] = self.plural

        template = self.get_template()
        rendered = template.safe_substitute(**kwargs)

        if f'${self.ARTICLE_TOKEN} ' in rendered:
            return self._render_articles(rendered)
        return rendered

    def get_template(self, number=None):
        """Get template based on given number, defaulting to current"""
        number = number if number is not None else self.number
        return self.template_map.get(number, self.template_map[self.NUMBER_TOKEN])

    def _render_articles(self, template):
        """Render all article tokens in the given template"""
        article_token = f'${self.ARTICLE_TOKEN}'
        words = template.split(' ')

        for i, word in enumerate(words):
            if word != article_token:
                continue
            try:
                next_word = words[i + 1]
            except IndexError:
                raise ValueError(f'Each article token ($a) must precede a word: {template}')
            article = 'an' if FIRST_SOUND_GUIDE.led_by_vowel_sound(next_word) else 'a'
            words[i] = article

        return ' '.join(words)

    def __add__(self, other):
        """Cast to string when added to a string from the left"""
        return str(self) + other

    def __radd__(self, other):
        """Cast to string when added to a string from the right"""
        return other + str(self)

    def __eq__(self, other):
        """Equality based on equality of members"""
        if isinstance(other, self.__class__):
            return (self.number == other.number and
                    self.singular == other.singular and
                    self.plural == other.plural and
                    self.template_map == other.template_map)
        return NotImplemented

    def __ne__(self, other):
        """Inequality based on inequality of members"""
        if isinstance(other, self.__class__):
            return not (self == other)
        return NotImplemented

    def __format__(self, formatter):
        """Format instance by passing args as a ;-delimited string"""
        if not formatter:
            return str(self)
        substrings = formatter.split(self.FORMATTER_DELIMITER)
        args = (self._deformat(substring) for substring in substrings)
        return str(self(*args))

    @property
    def is_complete(self):
        """True iff number, singular, and plural values are populated"""
        return bool((self.number is not None) and self.singular and self.plural)

    @property
    def formatter(self):
        """Construct formatter for current configuration"""
        return self.FORMATTER_DELIMITER.join(self.formatters)

    @property
    def formatters(self):
        """Construct list of formatters for current configuration"""
        return self._build_formatters(self.Formatter)

    @property
    def custom_formatter(self):
        """Construct formatter, excluding default templates"""
        return self.FORMATTER_DELIMITER.join(self.custom_formatters)

    @property
    def custom_formatters(self):
        """Construct list of formatters, excluding default templates"""
        return self._build_formatters(self.CustomFormatter)

    def _build_formatters(self, formatter_enum):
        """Construct list of formatters given a formatter enum"""
        formatters = []
        formatter_names = (formatter_option.value for formatter_option in formatter_enum)
        for formatter_name in formatter_names:
            formatter = getattr(self, formatter_name)
            if formatter:
                formatters.append(formatter)
        return formatters

    @property
    def number_formatter(self):
        """Construct number formatter from number value"""
        return str(self.number) if self.number is not None else None

    @property
    def forms(self):
        """Shorthand for form_formatter"""
        return self.form_formatter

    @property
    def form_formatter(self):
        """Construct form formatter from singular/plural values"""
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
    def templates(self):
        """Shorthand for template_formatter"""
        return self.template_formatter

    @property
    def template_formatter(self):
        """Construct template formatter from templates"""
        return self.FORMATTER_DELIMITER.join(self.template_formatters)

    @property
    def template_formatters(self):
        """Construct sorted list of template formatters"""
        return sorted(f'{k}{self.TEMPLATE_ASSIGNER}{v.template}'
                      for k, v in self.template_map.items())

    @property
    def custom_templates(self):
        """Shorthand for custom_template_formatter"""
        return self.custom_template_formatter

    @property
    def custom_template_formatter(self):
        """Construct template formatter, excluding default templates"""
        return self.FORMATTER_DELIMITER.join(self.custom_template_formatters)

    @property
    def custom_template_formatters(self):
        """Construct sorted list of template formatters, excluding defaults"""
        return sorted(f'{k}{self.TEMPLATE_ASSIGNER}{v.template}'
                      for k, v in self.custom_template_items)

    @property
    def custom_template_map(self):
        """Construct map of custom templates (excluding defaults)"""
        return dict(self.custom_template_items)

    @property
    def custom_template_items(self):
        """Return generator of custom templates (excluding defaults)"""
        return ((k, v) for k, v in self.template_map.items() if not self.is_default_template(k, v))

    def is_default_template(self, key, template=None):
        """True iff the specified template equals a default template"""
        template = template or self.template_map[key]
        default_template = self.TEMPLATE_DEFAULTS.get(key)
        return template == default_template

    @classmethod
    def is_template_formatter(cls, formatter):
        """True iff the given formatter is for a template"""
        return cls.TEMPLATE_ASSIGNER in formatter

    def _deformat(self, formatter):
        """Deformat number formatter to number, leaving others as strings"""
        if self.TEMPLATE_ASSIGNER in formatter:
            return formatter
        if self.FORM_DELIMITER in formatter:
            return formatter
        try:
            return ast.literal_eval(formatter)
        except ValueError:
            return formatter

    def _configure_from_args(self, *args, override=False):
        """Configure instance from given args"""
        templates_copied = number_configured = forms_configured = False
        for arg in args:
            if isinstance(arg, Number):
                self._configure_number(arg, number_configured, override)
                number_configured = True
            elif isinstance(arg, str):
                if self.is_template_formatter(arg):
                    if not templates_copied:
                        self.template_map = self.template_map.copy()
                        templates_copied = True
                    self._configure_templates(arg)
                else:
                    self._configure_forms(arg, forms_configured, override)
                    forms_configured = True
            else:
                raise TypeError('Arguments must be numbers or strings')

    def _configure_number(self, number, is_configured=False, override=False):
        """Configure instance with given number"""
        if is_configured or (not override and self.number is not None):
            raise ValueError('Number has already been configured')
        self.number = number

    def _configure_templates(self, formatter):
        """Configure instance with given template formatter"""
        if formatter:
            for sub_formatter in formatter.split(self.FORMATTER_DELIMITER):
                try:
                    key, value = sub_formatter.split(self.TEMPLATE_ASSIGNER)
                except ValueError:
                    raise ValueError(f'Invalid template formatter: {sub_formatter!r}')
                if key != self.NUMBER_TOKEN:
                    key = ast.literal_eval(key)
                self.template_map[key] = self.TEMPLATE_CLASS(value)

    def _configure_forms(self, formatter, is_configured=False, override=False):
        """Configure instance with given (singular/plural) form formatter"""
        singular, plural = self._derive_forms(formatter)
        if is_configured or (not override and (self.singular or self.plural)):
            raise ValueError('Singular/plural forms have already been configured')
        self.singular, self.plural = singular, plural

    def _derive_forms(self, formatter):
        """Derive singular and plural forms from form formatter"""
        base, _, suffixes = formatter.partition(self.FORM_DELIMITER)
        singular_suffix, _, plural_suffix = suffixes.rpartition(self.FORM_DELIMITER)
        singular = base + singular_suffix
        plural = base + plural_suffix
        return singular, plural

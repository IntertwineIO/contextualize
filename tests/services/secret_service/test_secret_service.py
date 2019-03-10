from unittest.mock import Mock, patch
from urllib.parse import urljoin

import pytest

from contextualize.extraction.configuration import PaginationConfiguration
from contextualize.services.secret_service.agency import SecretAgent, SecretService
from settings import TEST_WEB_SERVER_BASE_URL

TEST_USER_AGENT_RELATIVE_URL = '/useragents/explore/software_name/chrome/1.html'
TEST_USER_AGENT_URL = urljoin(TEST_WEB_SERVER_BASE_URL, TEST_USER_AGENT_RELATIVE_URL)


@pytest.mark.unit
def test_secret_service_core_operations(seed):
    file_path = 'tests/services/secret_service/chrome_agents.csv'
    service = SecretService(browser='chrome', file_path=file_path)
    agent_data = service._data[service.browser]
    assert len(agent_data) == 10

    user_agent = service.random
    validate_user_agent(user_agent)
    validate_random_variation(service, 'random', user_agent)

    agent = service.random_agent
    validate_agent(agent)
    validate_random_variation(service, 'random_agent', agent.user_agent)


def validate_agent(agent):
    assert isinstance(agent, SecretAgent)
    assert agent.source_url.startswith('http')
    assert agent.browser == 'Chrome'
    assert int(agent.browser_version.split('.')[0]) >= 44
    assert agent.operating_system in {'Windows', 'Linux'}
    assert agent.hardware_type == 'Computer'
    assert agent.popularity == 'Very common'
    validate_user_agent(agent.user_agent)


def validate_user_agent(user_agent):
    assert user_agent.startswith('Mozilla/5.0')
    assert 'AppleWebKit/537.36 (KHTML, like Gecko)' in user_agent


def validate_random_variation(service, property_name, compare_value):
    for _ in range(100):
        random_value = getattr(service, property_name)
        user_agent = random_value if property_name == 'random' else random_value.user_agent
        if user_agent != compare_value:
            validate_user_agent(user_agent)
            break
    else:
        raise ValueError('Either secret service is not random or a 1 in a googol event occurred')


@pytest.mark.integration
def test_user_agent_extraction(web_server):
    file_path = 'tests/services/secret_service/chrome_agents.csv'
    service = SecretService(browser='chrome', file_path=file_path)

    def mock_get_dict_side_effect(configuration):
        pagination_dict = configuration.get(PaginationConfiguration.PAGINATION_TAG)
        pagination_dict['pages'] = 2
        pagination_dict['page_size'] = 5
        return pagination_dict

    base_path = 'contextualize.extraction'
    with patch(f'{base_path}.url.URLConstructor.construct') as mock_url_construct, \
         patch(f'{base_path}.configuration.PaginationConfiguration.get_dict') as mock_get_dict, \
         patch('contextualize.utils.statistics.HumanDwellTime.random_delay') as mock_random_delay:

        mock_url_construct.return_value = TEST_USER_AGENT_URL
        mock_get_dict.side_effect = mock_get_dict_side_effect
        mock_random_delay.return_value = 0
        service.extract_data()

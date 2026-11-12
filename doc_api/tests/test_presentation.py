"""Public page build identity and Swagger URL handling without database startup."""
import httpx
import pytest

from doc_api.api.main import app
from doc_api.config import config
from doc_api.api.presentation import api_contact, api_description


@pytest.mark.parametrize('commit', [None, '1234567890abcdef1234567890abcdef12345678'])
@pytest.mark.asyncio
async def test_monitor_commit_badge(monkeypatch, commit):
    monkeypatch.setattr(config, 'APP_GIT_COMMIT', commit)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        monitor = await client.get('/monitor')
    for response in (monitor,):
        assert response.status_code == 200
        assert response.headers['cache-control'] == 'no-store'
        if commit:
            assert f'title="Commit: {commit}">{commit[:7]}</a>' in response.text
            assert f'href="https://github.com/DCGM/DocAPI/commit/{commit}"' in response.text
        else:
            assert 'title="Build commit unavailable">unavailable</span>' in response.text


@pytest.mark.parametrize('version,tag', [('v1.2.1', 'v1.2.1'), ('1.2.1', 'v1.2.1'),
                                       ('v1.3.0-rc.1', 'v1.3.0-rc.1'), ('local', None), ('custom', None)])
@pytest.mark.asyncio
async def test_monitor_version_release_link(monkeypatch, version, tag):
    monkeypatch.setattr(config, 'APP_VERSION', version)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        pages = [await client.get('/monitor')]
    for response in pages:
        assert response.status_code == 200
        if tag:
            assert f'href="https://github.com/DCGM/DocAPI/releases/tag/{tag}"' in response.text
            assert f'title="Release: {tag}">{version}</a>' in response.text
        else:
            assert f'title="No release for this build">{version}</span>' in response.text
            assert '/releases/tag/' not in response.text


@pytest.mark.asyncio
async def test_swagger_root_path_and_version(monkeypatch):
    monkeypatch.setattr(app, 'servers', [])
    monkeypatch.setattr(app, 'openapi_schema', None)
    transport = httpx.ASGITransport(app=app, root_path='/prefix')
    async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
        response = await client.get('/docs')
        redirect = await client.get('/docs/oauth2-redirect')
        schema = await client.get('/openapi.json')
    assert response.status_code == 200
    assert "url: '/prefix/openapi.json'" in response.text
    assert "/prefix/docs/oauth2-redirect" in response.text
    assert redirect.status_code == 200
    assert schema.json()['info']['version'] == app.version


@pytest.mark.parametrize('api_url,worker_url', [('', ''), ('https://github.com/DCGM/DocAPI', ''),
    ('', 'https://github.com/DCGM/MetaKat'),
    ('https://github.com/DCGM/DocAPI', 'https://github.com/DCGM/MetaKat/')])
@pytest.mark.asyncio
async def test_native_swagger_description(monkeypatch, api_url, worker_url):
    monkeypatch.setattr(config, 'API_REPOSITORY_URL', api_url)
    monkeypatch.setattr(config, 'WORKER_REPOSITORY_URL', worker_url)
    monkeypatch.setattr(app, 'description', api_description())
    monkeypatch.setattr(app, 'openapi_schema', None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        page = await client.get('/docs')
        schema = await client.get('/openapi.json')
    assert page.status_code == 200
    assert 'MutationObserver' not in page.text
    assert 'build-commit-badge' not in page.text
    description = schema.json()['info'].get('description', '')
    for label, url in [('API', api_url), ('Worker', worker_url)]:
        if url:
            url = url.rstrip('/')
            assert f'{label}: [Git repository]({url})' in description
            assert f'[Report a problem or suggest a change]({url}/issues/new)' in description
        else:
            assert f'{label}:' not in description
    assert '/commit/' not in description
    assert '/releases/tag/' not in description


@pytest.mark.parametrize('value,expected', [
    ('', None), ('   ', None),
    (' admin@example.com ', {'name': 'for API key requests: admin@example.com', 'email': 'admin@example.com'}),
    ('https://example.com/contact', {'name': 'for API key requests: https://example.com/contact', 'url': 'https://example.com/contact'}),
    ('Support team', {'name': 'for API key requests: Support team'}),
])
@pytest.mark.asyncio
async def test_native_swagger_contact(monkeypatch, value, expected):
    monkeypatch.setattr(config, 'CONTACT_TO_GET_NEW_KEY', value)
    monkeypatch.setattr(app, 'contact', api_contact())
    monkeypatch.setattr(app, 'openapi_schema', None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.get('/openapi.json')
    assert response.status_code == 200
    assert response.json()['info'].get('contact') == expected

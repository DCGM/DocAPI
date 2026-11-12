"""Build identity shared by the monitoring and Swagger pages."""
from html import escape
import re
from urllib.parse import quote

from doc_api.config import config

REPOSITORY_URL = 'https://github.com/DCGM/DocAPI'


def version_badge_html() -> str:
    version = config.APP_VERSION
    if re.fullmatch(r'v?\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?', version):
        tag = version if version.startswith('v') else 'v' + version
        return (f'<a class="version release" href="{REPOSITORY_URL}/releases/tag/{quote(tag, safe="")}" '
                f'target="_blank" rel="noopener noreferrer" title="Release: {escape(tag)}">{escape(version)}</a>')
    return f'<span class="version release" title="No release for this build">{escape(version)}</span>'


def commit_badge_html() -> str:
    commit = config.APP_GIT_COMMIT
    if commit:
        return (f'<a class="version commit" href="{REPOSITORY_URL}/commit/{quote(commit, safe="")}" '
                f'target="_blank" rel="noopener noreferrer" title="Commit: {escape(commit)}">'
                f'{escape(commit[:7])}</a>')
    return '<span class="version commit" title="Build commit unavailable">unavailable</span>'



def api_description() -> str:
    sections = []
    for label, repository in (('API', config.API_REPOSITORY_URL),
                              ('Worker', config.WORKER_REPOSITORY_URL)):
        if repository:
            url = quote(repository.rstrip('/'), safe=':/%?=&#+@~')
            sections.append(f'{label}: [Git repository]({url}) · '
                            f'[Report a problem or suggest a change]({url}/issues/new)')
    return '\n\n'.join(sections)


def api_contact() -> dict[str, str] | None:
    contact = config.CONTACT_TO_GET_NEW_KEY.strip()
    if not contact:
        return None
    if re.fullmatch(r'[^@\s]+@[^@\s]+', contact):
        return {'name': f'for API key requests: {contact}', 'email': contact}
    if contact.startswith(('https://', 'http://')):
        return {'name': f'for API key requests: {contact}', 'url': contact}
    return {'name': f'for API key requests: {contact}'}

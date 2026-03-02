import enum
import json
import time
from typing import List, Dict, Any
from http import HTTPStatus

import requests
from bs4 import BeautifulSoup

from . import guard
from .exceptions import ConfirmationExpected
from .login import InvalidCredentials

class Confirmation:
    """Represents a Steam mobile confirmation."""
    def __init__(self, data_confid: str, nonce: str) -> None:
        self.data_confid = data_confid
        self.nonce = nonce

class Tag(enum.Enum):
    CONF = 'conf'
    DETAILS = 'details'
    ALLOW = 'allow'
    CANCEL = 'cancel'

class ConfirmationExecutor:
    """Handles fetching and sending Steam mobile confirmations."""
    CONF_URL = 'https://steamcommunity.com/mobileconf'

    def __init__(self, identity_secret: str, my_steam_id: str, session: requests.Session) -> None:
        self._my_steam_id = my_steam_id
        self._identity_secret = identity_secret
        self._session = session

    def send_trade_allow_request(self, trade_offer_id: str) -> Dict[str, Any]:
        """Send allow request for a specific trade offer confirmation."""
        confirmations = self._get_confirmations()
        confirmation = self._select_trade_offer_confirmation(confirmations, trade_offer_id)
        return self._send_confirmation(confirmation)

    def _send_confirmation(self, confirmation: Dict[str, Any], cancel: bool = False) -> Dict[str, Any]:
        """Send (allow or cancel) a confirmation."""
        tag = Tag.ALLOW if not cancel else Tag.CANCEL
        params = self._create_confirmation_params(tag.value)
        params['op'] = (tag.value,)
        params['cid'] = confirmation['id']
        params['ck'] = confirmation['nonce']
        headers = {'X-Requested-With': 'XMLHttpRequest'}
        try:
            return self._session.get(f'{self.CONF_URL}/ajaxop', params=params, headers=headers).json()
        except Exception as e:
            raise RuntimeError(f"Failed to send confirmation: {e}")

    def _get_confirmations(self) -> List[Dict[str, Any]]:
        """Fetch all pending confirmations."""
        confirmations_page = self._fetch_confirmations_page()
        if confirmations_page.status_code == HTTPStatus.OK:
            try:
                confirmations_json = json.loads(confirmations_page.text)
                if 'conf' not in confirmations_json:
                    # If 'conf' key is missing, it might indicate session expired or other issues
                    raise ConfirmationExpected('Response missing "conf" key - session may have expired')
                return confirmations_json['conf']
            except json.JSONDecodeError as e:
                raise ConfirmationExpected(f'Failed to parse confirmations response: {e}')
            except KeyError as e:
                raise ConfirmationExpected(f'Response missing required key: {e}')
        else:
            raise ConfirmationExpected('Failed to fetch confirmations page.')

    def _fetch_confirmations_page(self) -> requests.Response:
        """Fetch the confirmations page from Steam."""
        tag = Tag.CONF.value
        params = self._create_confirmation_params(tag)
        headers = {'X-Requested-With': 'com.valvesoftware.android.steam.community'}
        response = self._session.get(f'{self.CONF_URL}/getlist', params=params, headers=headers)
        if 'Steam Guard Mobile Authenticator is providing incorrect Steam Guard codes.' in response.text:
            raise InvalidCredentials('Invalid Steam Guard file')
        return response

    def _fetch_confirmation_details_page(self, confirmation: Dict[str, Any]) -> str:
        """Fetch the HTML details page for a specific confirmation."""
        tag = f'details{confirmation["data_confid"]}'
        params = self._create_confirmation_params(tag)
        response = self._session.get(f'{self.CONF_URL}/details/{confirmation["data_confid"]}', params=params)
        try:
            response_json = response.json()
            if 'html' not in response_json:
                raise ConfirmationExpected('Response missing "html" key')
            return response_json['html']
        except json.JSONDecodeError as e:
            raise ConfirmationExpected(f'Failed to parse confirmation details response: {e}')

    def _create_confirmation_params(self, tag_string: str) -> Dict[str, Any]:
        """Create the parameters required for confirmation requests."""
        timestamp = int(time.time())
        confirmation_key = guard.generate_confirmation_key(self._identity_secret, tag_string, timestamp)
        android_id = guard.generate_device_id(self._my_steam_id)
        return {
            'p': android_id,
            'a': self._my_steam_id,
            'k': confirmation_key,
            't': timestamp,
            'm': 'android',
            'tag': tag_string,
        }

    def _select_trade_offer_confirmation(self, confirmations: List[Dict[str, Any]], trade_offer_id: str) -> Dict[str, Any]:
        """Select the confirmation matching the given trade offer ID."""
        for confirmation in confirmations:
            confirmation_details_page = self._fetch_confirmation_details_page(confirmation)
            confirmation_id = self._get_confirmation_trade_offer_id(confirmation_details_page)
            if confirmation_id == trade_offer_id:
                return confirmation
        raise ConfirmationExpected('No matching trade offer confirmation found.')

    @staticmethod
    def _get_confirmation_trade_offer_id(confirmation_details_page: str) -> str:
        """Extract the trade offer ID from the confirmation details HTML."""
        soup = BeautifulSoup(confirmation_details_page, 'html.parser')
        full_offer_id = soup.select('.tradeoffer')[0]['id']
        return full_offer_id.split('_')[1]
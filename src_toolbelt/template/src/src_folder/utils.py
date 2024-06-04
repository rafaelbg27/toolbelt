import logging

import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackBot:

    def __init__(self, slack_token: str) -> None:
        self.client = WebClient(token=slack_token)
        self.channels = None
        self.users = None

    def send_message(self, channel_id: str, message: str) -> None:
        try:
            self.client.chat_postMessage(channel=channel_id, text=message)
        except SlackApiError as e:
            logging.error(f"Error sending message: {e}")

    def _get_channels(self) -> pd.DataFrame:
        try:
            channels_store = {}
            cursor = None

            while cursor != "":
                result = self.client.conversations_list(limit=1000, cursor=cursor)
                cursor = result["response_metadata"]["next_cursor"]
                for channel in result["channels"]:
                    channel_id = channel["id"]
                    channels_store[channel_id] = channel

            df_channels = pd.DataFrame.from_dict(channels_store, orient="index")
            return df_channels.reset_index(drop=True)

        except SlackApiError as e:
            logging.error(f"Error getting channels: {e}")
            return pd.DataFrame()

    def _get_users(self) -> pd.DataFrame:
        try:
            users_store = {}
            cursor = None

            while cursor != "":
                result = self.client.users_list(limit=1000, cursor=cursor)
                cursor = result["response_metadata"]["next_cursor"]
                for user in result["members"]:
                    user_id = user["id"]
                    users_store[user_id] = user

            df_users = pd.DataFrame.from_dict(users_store, orient="index")
            df_users = pd.concat(
                [
                    df_users.drop(["profile"], axis=1),
                    df_users["profile"].apply(pd.Series),
                ],
                axis=1,
            )
            return df_users.reset_index(drop=True)

        except SlackApiError as e:
            logging.error(f"Error getting users: {e}")
            return pd.DataFrame()

    def get_user_id(self, user_name: str) -> str:
        if self.users is None:
            self.users = self._get_users()
        user_id = self.users[self.users["name"] == user_name]["id"].values[0]
        if user_id is None:
            logging.error(f"User {user_name} not found")
        return user_id

    def get_channel_id(self, channel_name: str) -> str:
        if self.channels is None:
            self.channels = self._get_channels()
        channel_id = self.channels[self.channels["name"] == channel_name]["id"].values[
            0
        ]
        if channel_id is None:
            logging.error(f"Channel {channel_name} not found")
        return channel_id

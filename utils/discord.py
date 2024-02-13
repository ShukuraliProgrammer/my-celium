import os, json
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import requests
import logging

load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)

# webhook_url = (
#     "https://discord.com/api/webhooks/"
#     + os.getenv("DISCORD_WEBHOOK_KEY")
#     + "/"
#     + os.getenv("DISCORD_WEBHOOK_SECRET")
# )


def submit_data_to_discord(webhook_url, message, embeds=None):
    if embeds:
        data = embeds
    else:
        data = {
            "username": "dev-bot",
            "content": message,
        }

    result = requests.post(webhook_url, json=data)

    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logger.error(err)
    else:
        logger.debug(f"Payload delivered successfully, code {result.status_code}.")

    return


class Discord:
    def send_embed(
        self, job_id, exception=None, execution_message: dict = None, add_author=False
    ):

        railway_fontend_link = os.getenv("RAILWAY_SERVICE_FRONTEND_URL")
        railway_fontend_url = f"https://{railway_fontend_link}"

        if railway_fontend_link:
            app_name = railway_fontend_link.split(".")[0]
        else:
            app_name = "NO APP NAME"

        green = 5763719
        red = 15548997
        color = green if not exception else red
        status_emoji = ":white_check_mark:" if not exception else ":red_square:"

        commit_message = os.getenv("RAILWAY_GIT_COMMIT_MESSAGE")
        git_repo_owner = os.getenv("RAILWAY_GIT_REPO_OWNER")
        git_repo_name = os.getenv("RAILWAY_GIT_REPO_NAME")
        git_commit_sha = os.getenv("RAILWAY_GIT_COMMIT_SHA")
        github_repo_link = f"https://github.com/{git_repo_owner}/{git_repo_name}/commit/{git_commit_sha}"

        railway_project_id = os.getenv("RAILWAY_PROJECT_ID")
        railway_service_id = os.getenv("RAILWAY_SERVICE_ID")
        railway_deploymment_id = os.getenv("RAILWAY_DEPLOYMENT_ID")

        railway_project_url = f"https://railway.app/project/{railway_project_id}/service/{railway_service_id}?id={railway_deploymment_id}"

        timestamp = datetime.utcnow().isoformat()

        if exception:
            exception_name = exception.__class__.__name__
        else:
            exception_name = str(None)

        payload = {
            "channel_id": "",
            "content": "",
            "tts": False,
            "embeds": [
                {
                    "type": "rich",
                    "title": f"{status_emoji} {app_name}",
                    "description": git_repo_name,
                    "color": color,
                    "fields": [
                        {"name": "Job ID", "value": job_id, "inline": True},
                        {"name": "Exception", "value": exception_name, "inline": True},
                        {
                            "name": "Deployment",
                            "value": f"[See logs]({railway_project_url})",
                            "inline": True,
                        },
                        {
                            "name": "Commit",
                            "value": f"[{commit_message}]({github_repo_link})",
                            "inline": False,
                        },
                    ],
                    "timestamp": timestamp,
                    "url": railway_fontend_url,
                }
            ],
        }

        if add_author:
            payload["autor"] = {
                "author": {
                    "name": git_repo_owner,
                    "url": github_repo_link,
                    "icon_url": "https://avatars.githubusercontent.com/u/91411128?v=4",
                }
            }

        if execution_message or exception:

            if exception:
                execution_message = " | ".join(exception.args)

            payload.get("embeds")[0].get("fields").append(
                {"name": "Execution", "value": execution_message, "inline": False}
            )
        pass
        submit_data_to_discord(webhook_url, message="", embeds=payload)
        return

    @staticmethod
    def log(message):
        submit_data_to_discord(webhook_url, message)
        return

    @staticmethod
    def debug(message):
        submit_data_to_discord(webhook_url, "**DEBUG**: " + message)
        return

    @staticmethod
    def critical(message):
        submit_data_to_discord(webhook_url, "**INFO**: " + message)
        return

    @staticmethod
    def warning(message):
        submit_data_to_discord(webhook_url, "**WARNING**: " + message)
        return

    @staticmethod
    def error(message):
        submit_data_to_discord(webhook_url, "**ERROR**: " + message)
        return

    @staticmethod
    def critical(message):
        submit_data_to_discord(webhook_url, "**CRITICAL**: " + message)
        return


if __name__ == "__main__":
    discord = Discord()
    discord.send_embed(job_id="fetched_latest_data", exception=None)
    # discord.critical("Hello World.")

import json
import re
import time
import traceback
from datetime import datetime
from hashlib import md5
from io import BytesIO
from logging import getLogger
from typing import Any, Generator, List, Tuple

import coloredlogs
import pymongo
import requests
import toml
from oauthlib import oauth1
from PIL import Image, ImageDraw, ImageFont

coloredlogs.install()

logger = getLogger("switch-predb")
config = toml.load("config.toml")


if config["render"]["renderer"] == "pyansi":
    from tempfile import NamedTemporaryFile

    from pyansilove.pyansilove import AnsiLove, Path
    from pyansilove.schemas import AnsiLoveOptions
    
elif config["render"]["renderer"] == "infekt":
    from tempfile import NamedTemporaryFile

    import subprocess


OLD_HASH_SET = set()

DEBUG = config["common"]["debug"]
NEWLINE = "\n"
TITLE_ID_BASE_MASK = 0xFFFFFFFFFFFFE000
TITLE_ID_REGEX = re.compile(r"01[A-Fa-f0-9X]{12,}")
ONE_MINUTE = 60
ONE_HOUR = 60 * ONE_MINUTE
CACHE = {
    "releases": {},
    "nfos": {}
}
COLORS = {
        "info": 0x00ffe0,
        "warning": 0xf6ff00,
        "error": 0xe86998,
        "critical": 0xff0000
    }

SRRDB_SCAN_URL = "https://api.srrdb.com/v1/search/category:nsw/order:date-desc"
SRRDB_RELEASE_URL = "https://api.srrdb.com/v1/details/{release_name}"
SRRDB_FILE_URL = "https://www.srrdb.com/download/file/{release_name}/{file_name}"
TINFOIL_URL = "https://tinfoil.media/ti/{title_id}/1024/1024/"

TWITTER_BASE_URL = "https://twitter.com"
TWITTER_MEDIA_ENDPOINT_URL = 'https://upload.twitter.com/1.1/media/upload.json'
POST_TWEET_URL = 'https://api.twitter.com/2/tweets'


mongo_client = pymongo.MongoClient(
    host=config["mongo"]["url"],
)[config["mongo"]["collection"]]


def format_exception(exception: Exception) -> str:
    return ''.join(traceback.format_exception(None, exception, exception.__traceback__))


def make_logging_message(level: str, message: str) -> dict:
    return {
        "content": None,
        "embeds":
            [{
                "title": "New Logging Message",
                "description": message,
                "color": COLORS.get(level),
                "timestamp": datetime.fromtimestamp(time.time() - (2 * ONE_HOUR)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }],
        "attachments": []
    }


def log_discord(level: str, message: str):
    requests.post(
        config["discord"]["webhook"],
        json = make_logging_message(level, message),
        timeout=10
    )


def create_ntfy_action(label: str, url: str) -> dict:
    return {
        "action": "view",
        "label": label,
        "url": url
    }


def log_ntfy(message: str, public: bool = False, actions: list = []):
     requests.post(
        config["ntfy"]["server"],
        headers={
            "Authorization": f"Bearer {config['ntfy']['token']}",
        },
        data = json.dumps({
            "topic": config["ntfy"]["topic"] if not public else config["ntfy"]["public_topic"],
            "message": message,
            "title": "New Logging Message",
            "markdown": True,
            "actions": actions
        }),
        timeout=10
    )


def log(level: str, message: str, *, silent: bool = False, publish: bool = False, ntfy_actions: list = []):
    getattr(logger, level)(message)

    if silent:
        return
    
    if config["discord"]["enabled"]:
        log_discord(level, message)

    if config["ntfy"]["enabled"]:
        log_ntfy(message, actions=ntfy_actions)

    if publish and config["ntfy"]["enabled"]:
        log_ntfy(message, publish, actions=ntfy_actions)


def request_url(
        url: str,
        caller_name: str,
        method: str ="get",
        default: Any = None,
        apply: str = None,
        apply_kwargs: dict = {},
        **kwargs
    ):
    if DEBUG:
        log("info", f"[REQ][{caller_name}] Reaching {url}, method: {method}, apply: {apply}, apply_kwargs: {apply_kwargs}, kwargs: {kwargs}")
    try:
        response: requests.Response = getattr(requests, method)(url, timeout=10, **kwargs)

    except requests.RequestException as exception:
        log("error", f"[REQ][{caller_name}] Reaching {url} failed: ```{format_exception(exception)}```")
        return default
    
    if response.status_code not in range(200, 299):
        log("error", f"[REQ][{caller_name}] Non-200 response: {response.status_code} - `{response.text}`")
        return default
    
    return getattr(response, apply)(**apply_kwargs) if apply else response


def scan_srrdb() -> dict:
    return request_url(SRRDB_SCAN_URL, "SCN", apply="json")["results"]


def find_new_releases(releases: List[dict]) -> Generator[dict, None, None]:
    initial = not OLD_HASH_SET and not DEBUG
    
    for release in releases:
        release_hash = md5(release["release"].encode()).hexdigest()

        if initial:
            OLD_HASH_SET.add(release_hash)
            continue

        if release_hash in OLD_HASH_SET:
            continue
        
        OLD_HASH_SET.add(release_hash)

        yield release


def get_details(release_name: str) -> dict:
    if release_name in CACHE["releases"]:
        return CACHE["releases"][release_name]
    
    details = request_url(SRRDB_RELEASE_URL.format(release_name=release_name), "DET", apply="json")
    CACHE["releases"][release_name] = details
    return details
 

def mask_title_id(title_id: str) -> str:
    return "0" + hex(
        int(title_id, 16) & TITLE_ID_BASE_MASK
    )[2:].upper()


def parse_nfo(nfo_url: str) -> Tuple[str, str]:
    log("info", f"[NFO] Parsing {nfo_url}")
    nfo = request_url(nfo_url, "NFO").content.decode("cp437")
    
    if not nfo:
        return

    title_id = TITLE_ID_REGEX.search(nfo)

    if not title_id:
        log("error", f"[NFO] Could not parse Title ID from {nfo_url}")
        return

    title_id = title_id.group().replace("X", "0")
    masked_title_id = mask_title_id(title_id)

    if title_id not in CACHE["nfos"]:
        CACHE["nfos"][title_id] = nfo

    return title_id, masked_title_id


def humansize(size: int) -> str:
    suffixes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    index = 0

    while size >= 1024 and index < len(suffixes)-1:
        size /= 1024.
        index += 1
    
    size = ('%.2f' % size).rstrip('0').rstrip('.')

    return '%s %s' % (size, suffixes[index])


def get_info(release_name: str) -> dict:
    details = get_details(release_name)

    if not details:
        return

    proof_url = None
    files = details["files"]

    if "Proof" in files[1]["name"]:
        proof_url = SRRDB_FILE_URL.format(release_name=release_name, file_name=files[1]["name"])

    nfo_url = SRRDB_FILE_URL.format(release_name=release_name, file_name=files[0]["name"])

    parse_result = parse_nfo(nfo_url)

    if not parse_result:
        return
    
    title_id, masked_title_id = parse_result

    size = humansize(details["archived-files"][0]["size"])
    crc = details["archived-files"][0]["crc"]

    return {
        "tid": title_id,
        "masked_tid": masked_title_id,
        "title": release_name,
        "size": size,
        "crc": crc,
        "proof": proof_url,
        "nfo": nfo_url,
        "thumb": TINFOIL_URL.format(title_id=masked_title_id)
    }


def add_to_mongo(release_info: dict):
    log("info", f"[MDB] Adding {release_info['title']} to MongoDB")
    mongo_client.releases.insert_one(release_info)


def render_nfo(release_info: dict) -> Image.Image:
    log("info", f"[NFO] Rendering {release_info['title']} NFO with custom renderer")

    nfo_font = ImageFont.truetype(
        config["render"]["font_path"],
        size=config["render"]["font_size"],
        encoding='unic'
    )

    nfo_font_height = config["render"]["font_size"]

    nfo_lines = CACHE["nfos"][release_info["tid"]].split("\n")
    longest_line = max(len(line) for line in nfo_lines)

    nfo_size = (
        config["render"]["font_width"] * longest_line,
        14 * len(nfo_lines)
    )

    rendered_nfo = Image.new(
        "RGB", nfo_size, config["render"]["background"])
    
    nfo_draw = ImageDraw.Draw(rendered_nfo)
    current_offset = 0

    for line in nfo_lines:
        nfo_draw.text(
            (0, current_offset),
            line,
            font=nfo_font,
            fill=config["render"]["foreground"]
        )
        current_offset += nfo_font_height
    
    return rendered_nfo


def render_nfo_pyansi(release_info: dict) -> Image.Image:
    log("info", f"[NFO] Rendering {release_info['title']} NFO with pyansi")
    with (
        NamedTemporaryFile(mode="w", suffix=".nfo") as nfo_file,
        NamedTemporaryFile(mode="w", suffix=".png") as image_file
    ):
        nfo_file.write(CACHE["nfos"][release_info["tid"]])
        nfo_file.flush()

        AnsiLove.ansi(
            input_path=Path(nfo_file.name),
            output_path=Path(image_file.name),
            options=AnsiLoveOptions(
                truecolor=True
            )
        )

        rendered_nfo = Image.open(image_file.name)
        rendered_nfo.load()
    
    return rendered_nfo


def render_nfo_infekt(release_info: dict) -> Image.Image:
    log("info", f"[NFO] Rendering {release_info['title']} NFO with infekt")

    with (
        NamedTemporaryFile(mode="w", suffix=".nfo") as nfo_file,
        NamedTemporaryFile(mode="r", suffix=".png") as image_file
    ):
        nfo_file.write(CACHE["nfos"][release_info["tid"]])
        nfo_file.flush()

        subprocess.run([
            "infekt-cli", nfo_file.name, "-O", image_file.name,
            "-T", "ffffff", "-B", "000000", "-c"
        ])

        rendered_nfo = Image.open(image_file.name)
        rendered_nfo.load()
    
    return rendered_nfo


def upload_nfo(release_info: dict, buffer: BytesIO, mode: str) -> str:
    log("info", f"[RNR] Uploading rendered NFO {release_info['title']}")

    response = request_url(
        config["zipline"]["url"],
        "RNR",
        method="post",
        apply="json", 
        files={"file": (f'{release_info["title"]}.{mode}', buffer, f"image/{mode}")},
        headers={"Authorization": config["zipline"]["token"],
                 "Image-Compression-Percent": "0",
                 "Format": "NAME"}
    )

    if not response:
        return
    
    url = response["files"][0]

    release_info["nfo"] = url

    return url


def make_twitter_post(release_info: dict) -> dict:
    title = release_info["title"]
    post = f"""
New Release by {title[title.rfind('-')+1:]}!
    
{title} [{release_info["tid"]}][{release_info["crc"]}]
Size: {release_info["size"]}

View on Tinfoil: https://tinfoil.io/Title/{release_info["masked_tid"]}
View on eShop: https://ec.nintendo.com/apps/{release_info["masked_tid"]}/US
"""
    
    return {
        "text": post,
        **({"media": {"media_ids": [str(image["media_id"]) for image in release_info["media"]]}} if release_info["media"] else {})
    }


def generate_oauth_headers(sign_headers: dict, url: str, content: dict = None, multipart: bool = False) -> dict:
    ouath_client = oauth1.Client(
        client_key              = config["twitter"]["consumer_key"],
        client_secret           = config["twitter"]["consumer_secret"],
        resource_owner_key      = config["twitter"]["access_token"],
        resource_owner_secret   = config["twitter"]["access_token_secret"],
    )
    _, headers, _ = ouath_client.sign(
        http_method = "POST",
        body = content,
        uri = url,
        headers=sign_headers
    ) if not multipart else ouath_client.sign(
        http_method = "POST",
        uri = url
    )

    return headers | sign_headers


def do_twitter_request(url: str, sign_headers: dict, caller_name: str, multipart: bool = False,  **kwargs) -> dict:
    return request_url(
        url,
        caller_name,
        method="post",
        headers=generate_oauth_headers(
            sign_headers,
            url,
            content = kwargs.get("data", None),
            multipart = multipart
        ),
        apply="json",
        **kwargs
    )


def upload_media(release_info: dict):
    log("info", f"[UMD] Uploading media for {release_info['title']}")

    release_info["media"] = []

    for image_type in ["thumb", "nfo", "proof"]:
        if not release_info[image_type]:
            continue

        log("info", f"[UMD] Uploading {image_type} media for {release_info['title']}: {release_info[image_type]}")

        media_info = {
            "url": release_info[image_type],
        }

        image_content = request_url(release_info[image_type], "UMD")

        if not image_content:
            continue

        with BytesIO(
            image_content.content
        ) as io:
            response = do_twitter_request(
                TWITTER_MEDIA_ENDPOINT_URL,
                {},
                caller_name = "UMD",
                files = {
                    "media": (f"{image_type}_{release_info['tid']}.jpg", io)
                }
            )

        if not response:
            continue
        
        release_info["media"].append(media_info | response)


def post_to_twitter(release_info: dict) -> Tuple[dict, dict]:
    log("info", f"[TWT] Posting to Twitter")

    upload_media(release_info)
    content = make_twitter_post(release_info)

    return content, do_twitter_request(
        POST_TWEET_URL,
        {"Content-Type": "application/json"},
        caller_name = "TWT",
        data = json.dumps(content)
    )


def handle_releases(releases: List[dict]) -> None:
    for release in releases:
        release_name = release["release"]
        log("info", f"[REL] Found new release: {release_name}", publish=True)

        if not release["hasNFO"]:
            log("warning", f"[REL] Release {release_name} has no NFO", publish=True)
            continue
        
        release_info = get_info(release_name)

        if not release_info:
            continue

        with BytesIO() as buffer:
            if config["render"]["renderer"] == "pyansi":
                mode = "png"
                render_nfo_pyansi(release_info).save(buffer, format=mode)

            elif config["render"]["renderer"] == "builtin":
                mode = "jpeg"
                render_nfo(release_info).save(buffer, format=mode)
            
            elif config["render"]["renderer"] == "infekt":
                mode = "png"
                render_nfo_infekt(release_info).save(buffer, format=mode)

            else:
                raise ValueError(f"Unknown renderer {config['render']['renderer']}")

            buffer.seek(0)

            if not upload_nfo(release_info, buffer, mode):
                continue
        
        if config["mongo"]["enabled"]:
            add_to_mongo(release_info)
        
        post, response = post_to_twitter(release_info)

        log(
            "info",
            f"[REL] {post['text']}\n\n{NEWLINE.join(media['url'] for media in release_info['media'])}",
            publish=True,
            ntfy_actions=[
                create_ntfy_action("View on Twitter", f"{TWITTER_BASE_URL}/{config['twitter']['username']}/status/{response['data']['id']}"),
                create_ntfy_action("View on Tinfoil", f"https://tinfoil.io/Title/{release_info['masked_tid']}"),
                create_ntfy_action("View on eShop", f"https://ec.nintendo.com/apps/{release_info['masked_tid']}/US")
                ]
        )

        if config["common"]["debug"]:
            return

        time.sleep(ONE_MINUTE)


def main_loop():
    log("info", "[MLP] Starting Main Loop")

    while True:
        releases = scan_srrdb()
        releases = find_new_releases(releases)
        handle_releases(releases)

        if config["common"]["debug"]:
            return

        time.sleep(ONE_MINUTE)


if __name__ == "__main__":
    main_loop()
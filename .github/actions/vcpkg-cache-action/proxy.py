import asyncio
import logging
import os

from aiohttp import (
    ClientError,
    ClientHandlerType,
    ClientRequest,
    ClientResponse,
    ClientSession,
    ClientTimeout,
)
from fastapi import FastAPI, Request, Response, status


MAX_RETRIES = 5


logger = logging.getLogger("uvicorn.error")
url = os.environ["ACTIONS_RESULTS_URL"]
token = os.environ["ACTIONS_RUNTIME_TOKEN"]
auth_headers = headers = {
    "Authorization": f"Bearer {token}",
}


async def retry_middleware(
    req: ClientRequest, handler: ClientHandlerType
) -> ClientResponse:
    for attempt in range(MAX_RETRIES):
        try:
            resp: ClientResponse = await handler(req)
            if resp.status < 500:
                return resp

            logger.warning(
                f"HTTP request failed with status {resp.status} on attempt {attempt + 1}"
            )
            if attempt + 1 == MAX_RETRIES:
                return resp

        except ClientError as e:
            logger.warning(f"HTTP request failed on attempt {attempt + 1}: {e}")
            if attempt + 1 == MAX_RETRIES:
                raise e

        await asyncio.sleep(2**attempt)


async def lifespan(app: FastAPI):
    app.state.session = ClientSession(
        timeout=ClientTimeout(sock_connect=10, sock_read=60),
        middlewares=[retry_middleware],
    )
    try:
        yield
    finally:
        await app.state.session.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return "OK"


class TwirpError(Exception):
    def __init__(self, status_code: int, media_type: str, body: bytes):
        self.status_code = status_code
        self.media_type = media_type
        self.body = body


async def twirp_call(
    session: ClientSession,
    method: str,
    payload: dict,
) -> dict:
    async with session.post(
        f"{url}twirp/github.actions.results.api.v1.CacheService/{method}",
        headers=auth_headers,
        json=payload,
    ) as resp:
        media_type = resp.headers.get("Content-Type")
        if resp.status != status.HTTP_200_OK:
            body = await resp.read()
            if resp.status != status.HTTP_404_NOT_FOUND:
                headers = "\n".join(f"{k}: {v}" for k, v in resp.headers.items())
                logger.warning(
                    f"{method} failed with status {resp.status}:\nheaders:\n{headers}\nbody:\n{body.decode()}"
                )
            raise TwirpError(resp.status, media_type, body)
        data = await resp.json()
        if not data.get("ok", False):
            body = await resp.read()
            if method != "GetCacheEntryDownloadURL":
                headers = "\n".join(f"{k}: {v}" for k, v in resp.headers.items())
                logger.warning(
                    f"{method} did not return ok:\nheaders:\n{headers}\nbody:\n{body.decode()}"
                )
            raise TwirpError(status.HTTP_404_NOT_FOUND, media_type, body)
        return data


@app.head("/{name}/{triplet}/{version}/{sha}")
async def head(name: str, triplet: str, version: str, sha: str, request: Request):
    try:
        await twirp_call(
            request.app.state.session,
            "GetCacheEntryDownloadURL",
            {
                "key": f"{name}_{triplet}_{version}",
                "version": sha,
            },
        )
    except TwirpError as e:
        return Response(status_code=e.status_code)


@app.get("/{name}/{triplet}/{version}/{sha}")
async def get(name: str, triplet: str, version: str, sha: str, request: Request):
    session: ClientSession = request.app.state.session

    try:
        data = await twirp_call(
            session,
            "GetCacheEntryDownloadURL",
            {
                "key": f"{name}_{triplet}_{version}",
                "version": sha,
            },
        )
        download_url = data["signed_download_url"]
    except TwirpError as e:
        return Response(e.body, status_code=e.status_code, media_type=e.media_type)

    async with session.get(download_url) as resp:
        media_type = resp.headers.get("Content-Type")
        if resp.status != status.HTTP_200_OK:
            body = await resp.read()
            headers = "\n".join(f"{k}: {v}" for k, v in resp.headers.items())
            logger.warning(
                f"Download failed with status {resp.status}:\nheaders:\n{headers}\nbody:\n{body.decode()}"
            )
            return Response(body, status_code=resp.status, media_type=media_type)
        return Response(await resp.read(), media_type=media_type)


@app.put("/{name}/{triplet}/{version}/{sha}")
async def put(
    name: str,
    triplet: str,
    version: str,
    sha: str,
    request: Request,
):
    session: ClientSession = request.app.state.session

    try:
        data = await twirp_call(
            session,
            "CreateCacheEntry",
            {
                "key": f"{name}_{triplet}_{version}",
                "version": sha,
            },
        )
        upload_url = data["signed_upload_url"]
    except TwirpError as e:
        return Response(e.body, status_code=e.status_code, media_type=e.media_type)

    content = await request.body()

    async with session.put(
        upload_url, headers={"x-ms-blob-type": "BlockBlob"}, data=content
    ) as resp:
        if resp.status != status.HTTP_201_CREATED:
            body = await resp.read()
            headers = "\n".join(f"{k}: {v}" for k, v in resp.headers.items())
            logger.warning(
                f"Upload failed with status {resp.status}:\nheaders:\n{headers}\nbody:\n{body.decode()}"
            )
            return Response(
                body,
                status_code=resp.status,
                media_type=resp.headers.get("Content-Type"),
            )

    try:
        await twirp_call(
            session,
            "FinalizeCacheEntryUpload",
            {
                "key": f"{name}_{triplet}_{version}",
                "version": sha,
                "sizeBytes": len(content),
            },
        )
    except TwirpError as e:
        return Response(e.body, status_code=e.status_code, media_type=e.media_type)

    return Response(status_code=status.HTTP_201_CREATED)

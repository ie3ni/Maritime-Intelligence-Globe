from __future__ import annotations

import asyncio
import email.utils
import os
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx


class HostRateLimiter:
    def __init__(self, max_concurrent: int = 2, minimum_interval: float = 0.5):
        self.minimum_interval = minimum_interval
        self._semaphores = defaultdict(lambda: asyncio.Semaphore(max_concurrent))
        self._locks = defaultdict(asyncio.Lock)
        self._last_request: dict[str, float] = {}

    async def __aenter_host__(self, host: str):
        semaphore = self._semaphores[host]
        await semaphore.acquire()
        async with self._locks[host]:
            wait = self.minimum_interval - (time.monotonic() - self._last_request.get(host, 0))
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request[host] = time.monotonic()
        return semaphore

    async def run(self, url: str, operation):
        sem = await self.__aenter_host__(urlparse(url).netloc)
        try:
            return await operation()
        finally:
            sem.release()


def retry_after_seconds(value: str | None, now: datetime | None = None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
            now = now or datetime.now(timezone.utc)
            return max(0.0, (parsed - now).total_seconds())
        except (TypeError, ValueError):
            return None


class PoliteHttpClient:
    RETRYABLE = {429, 500, 502, 503, 504}

    def __init__(self, config: dict):
        self.config = config
        self.limiter = HostRateLimiter(config["max_concurrent_requests_per_host"], config["minimum_request_interval_seconds"])
        self.client = httpx.AsyncClient(timeout=config["timeout_seconds"], follow_redirects=True)

    async def get(self, url: str, **kwargs) -> httpx.Response:
        for attempt in range(self.config["max_retries"] + 1):
            try:
                response = await self.limiter.run(url, lambda: self.client.get(url, **kwargs))
                if response.status_code not in self.RETRYABLE:
                    response.raise_for_status()
                    return response
                if attempt == self.config["max_retries"]:
                    response.raise_for_status()
                retry_after = retry_after_seconds(response.headers.get("Retry-After")) if self.config.get("honor_retry_after") else None
            except asyncio.CancelledError:
                temporary.unlink(missing_ok=True)
                raise
            except (httpx.TimeoutException, httpx.TransportError):
                if attempt == self.config["max_retries"]:
                    raise
                retry_after = None
            base = min(self.config["backoff_max_seconds"], self.config["backoff_initial_seconds"] * 2**attempt)
            await asyncio.sleep(retry_after if retry_after is not None else base * random.uniform(0.8, 1.2))
        raise RuntimeError("unreachable")

    async def download(
        self,
        url: str,
        target,
        *,
        maximum_bytes: int,
        **kwargs,
    ) -> dict[str, str]:
        """Stream one bounded response to disk using the shared polite limiter."""
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".part")
        for attempt in range(self.config["max_retries"] + 1):
            retry_after = None
            semaphore = await self.limiter.__aenter_host__(urlparse(url).netloc)
            try:
                async with self.client.stream("GET", url, **kwargs) as response:
                    if response.status_code in self.RETRYABLE:
                        if attempt == self.config["max_retries"]:
                            response.raise_for_status()
                        if self.config.get("honor_retry_after"):
                            retry_after = retry_after_seconds(response.headers.get("Retry-After"))
                    else:
                        response.raise_for_status()
                        declared = response.headers.get("Content-Length")
                        if declared and int(declared) > maximum_bytes:
                            raise RuntimeError(
                                f"Download exceeds configured limit of {maximum_bytes:,} bytes"
                            )
                        written = 0
                        try:
                            with temporary.open("wb") as handle:
                                async for chunk in response.aiter_bytes():
                                    written += len(chunk)
                                    if written > maximum_bytes:
                                        raise RuntimeError(
                                            f"Download exceeds configured limit of {maximum_bytes:,} bytes"
                                        )
                                    handle.write(chunk)
                            os.replace(temporary, target)
                        except Exception:
                            temporary.unlink(missing_ok=True)
                            raise
                        return dict(response.headers)
            except (httpx.TimeoutException, httpx.TransportError):
                temporary.unlink(missing_ok=True)
                if attempt == self.config["max_retries"]:
                    raise
            finally:
                semaphore.release()
            base = min(
                self.config["backoff_max_seconds"],
                self.config["backoff_initial_seconds"] * 2**attempt,
            )
            await asyncio.sleep(
                retry_after if retry_after is not None else base * random.uniform(0.8, 1.2)
            )
        raise RuntimeError("unreachable")

    async def aclose(self):
        await self.client.aclose()

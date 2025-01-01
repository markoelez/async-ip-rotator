import asyncio
import ipaddress
from random import choice, randint
from time import sleep
from typing import Any, Dict, List, Optional

import boto3
import botocore.exceptions
import httpx

MAX_IPV4 = ipaddress.IPv4Address._ALL_ONES

DEFAULT_REGIONS: List[str] = [
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
]

EXTRA_REGIONS: List[str] = DEFAULT_REGIONS + [
    "ap-south-1",
    "ap-northeast-3",
    "ap-northeast-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-northeast-1",
    "sa-east-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "ca-central-1",
]

ALL_REGIONS: List[str] = EXTRA_REGIONS + [
    "ap-east-1",
    "af-south-1",
    "eu-south-1",
    "me-south-1",
    "eu-north-1",
]


class AsyncApiGateway:
    def __init__(
        self,
        site: str,
        regions: List[str] = DEFAULT_REGIONS,
        access_key_id: Optional[str] = None,
        access_key_secret: Optional[str] = None,
        verbose: bool = True,
    ) -> None:
        if site.endswith("/"):
            self.site = site[:-1]
        else:
            self.site = site
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.api_name = self.site + " - IP Rotate API"
        self.regions = regions
        self.verbose = verbose
        self.endpoints: List[str] = []

    async def __aenter__(self) -> "AsyncApiGateway":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.shutdown()

    async def send(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 10.0,
        **request_kwargs: Any,
    ) -> httpx.Response:
        if headers is None:
            headers = {}
        if not self.endpoints:
            raise RuntimeError(
                "No API Gateway endpoints initialized. Call start() first."
            )
        endpoint = choice(self.endpoints)
        protocol, site_rest = url.split("://", 1)
        if "/" in site_rest:
            site_domain, site_path = site_rest.split("/", 1)
        else:
            site_domain = site_rest
            site_path = ""
        new_url = f"https://{endpoint}/ProxyStage/{site_path}"
        headers["Host"] = endpoint
        x_forwarded_for = headers.get("X-Forwarded-For")
        if x_forwarded_for is None:
            x_forwarded_for = ipaddress.IPv4Address._string_from_ip_int(
                randint(0, MAX_IPV4)
            )
        headers.pop("X-Forwarded-For", None)
        headers["X-My-X-Forwarded-For"] = x_forwarded_for
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method, new_url, headers=headers, **request_kwargs
            )
            return response

    async def start(
        self,
        force: bool = False,
        require_manual_deletion: bool = False,
        endpoints: Optional[List[str]] = None,
    ) -> List[str]:
        if endpoints:
            self.endpoints = endpoints
            return self.endpoints
        if self.verbose:
            print(
                f"Starting API gateway{'s' if len(self.regions) > 1 else ''} in {len(self.regions)} region(s)."
            )
        self.endpoints = []
        tasks = []
        for region in self.regions:
            tasks.append(
                self.init_gateway(
                    region, force=force, require_manual_deletion=require_manual_deletion
                )
            )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        new_endpoints = 0
        for result in results:
            if isinstance(result, dict) and result.get("success"):
                self.endpoints.append(result["endpoint"])
                if result["new"]:
                    new_endpoints += 1
            elif isinstance(result, Exception):
                if self.verbose:
                    print(f"Gateway creation task raised exception: {result}")
        if self.verbose:
            print(
                f"Using {len(self.endpoints)} endpoints with name '{self.api_name}' ({new_endpoints} new)."
            )
        return self.endpoints

    async def shutdown(self, endpoints: Optional[List[str]] = None) -> List[str]:
        if self.verbose:
            print(
                f"Deleting gateway{'s' if len(self.regions) > 1 else ''} for site '{self.site}'."
            )
        tasks = []
        for region in self.regions:
            tasks.append(self.delete_gateway(region, endpoints=endpoints))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        deleted_ids: List[str] = []
        for r in results:
            if isinstance(r, list):
                deleted_ids.extend(r)
            elif isinstance(r, Exception):
                if self.verbose:
                    print(f"Deletion task raised exception: {r}")
        if self.verbose:
            print(f"Deleted {len(deleted_ids)} endpoint(s) for site '{self.site}'.")
        return deleted_ids

    async def init_gateway(
        self, region: str, force: bool = False, require_manual_deletion: bool = False
    ) -> Dict[str, Any]:
        def _blocking_init() -> Dict[str, Any]:
            return self._init_gateway_sync(region, force, require_manual_deletion)

        return await asyncio.to_thread(_blocking_init)

    def _init_gateway_sync(
        self, region: str, force: bool = False, require_manual_deletion: bool = False
    ) -> Dict[str, Any]:
        session = boto3.session.Session()
        awsclient = session.client(
            "apigateway",
            region_name=region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key_secret,
        )
        if not force:
            try:
                current_apis = self.get_gateways_sync(awsclient)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "UnrecognizedClientException":
                    if self.verbose:
                        print(
                            f"Could not create region (requires manual enabling): {region}"
                        )
                    return {"success": False}
                else:
                    raise e
            for api in current_apis:
                if "name" in api and api["name"].startswith(self.api_name):
                    return {
                        "success": True,
                        "endpoint": f"{api['id']}.execute-api.{region}.amazonaws.com",
                        "new": False,
                    }
        new_api_name = self.api_name
        if require_manual_deletion:
            new_api_name += " (Manual Deletion Required)"
        create_api_resp = awsclient.create_rest_api(
            name=new_api_name,
            endpointConfiguration={"types": ["REGIONAL"]},
        )
        rest_api_id = create_api_resp["id"]
        root_resp = awsclient.get_resources(restApiId=rest_api_id)
        root_id = root_resp["items"][0]["id"]
        proxy_resp = awsclient.create_resource(
            restApiId=rest_api_id, parentId=root_id, pathPart="{proxy+}"
        )
        awsclient.put_method(
            restApiId=rest_api_id,
            resourceId=root_id,
            httpMethod="ANY",
            authorizationType="NONE",
            requestParameters={
                "method.request.path.proxy": True,
                "method.request.header.X-My-X-Forwarded-For": True,
            },
        )
        awsclient.put_integration(
            restApiId=rest_api_id,
            resourceId=root_id,
            type="HTTP_PROXY",
            httpMethod="ANY",
            integrationHttpMethod="ANY",
            uri=self.site,
            connectionType="INTERNET",
            requestParameters={
                "integration.request.path.proxy": "method.request.path.proxy",
                "integration.request.header.X-Forwarded-For": "method.request.header.X-My-X-Forwarded-For",
            },
        )
        awsclient.put_method(
            restApiId=rest_api_id,
            resourceId=proxy_resp["id"],
            httpMethod="ANY",
            authorizationType="NONE",
            requestParameters={
                "method.request.path.proxy": True,
                "method.request.header.X-My-X-Forwarded-For": True,
            },
        )
        awsclient.put_integration(
            restApiId=rest_api_id,
            resourceId=proxy_resp["id"],
            type="HTTP_PROXY",
            httpMethod="ANY",
            integrationHttpMethod="ANY",
            uri=f"{self.site}/{{proxy}}",
            connectionType="INTERNET",
            requestParameters={
                "integration.request.path.proxy": "method.request.path.proxy",
                "integration.request.header.X-Forwarded-For": "method.request.header.X-My-X-Forwarded-For",
            },
        )
        awsclient.create_deployment(restApiId=rest_api_id, stageName="ProxyStage")
        return {
            "success": True,
            "endpoint": f"{rest_api_id}.execute-api.{region}.amazonaws.com",
            "new": True,
        }

    async def delete_gateway(
        self, region: str, endpoints: Optional[List[str]] = None
    ) -> List[str]:
        def _blocking_delete() -> List[str]:
            return self._delete_gateway_sync(region, endpoints)

        return await asyncio.to_thread(_blocking_delete)

    def _delete_gateway_sync(
        self, region: str, endpoints: Optional[List[str]] = None
    ) -> List[str]:
        session = boto3.session.Session()
        awsclient = session.client(
            "apigateway",
            region_name=region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key_secret,
        )
        endpoint_ids: List[str] = []
        if endpoints is not None:
            for ep in endpoints:
                endpoint_ids.append(ep.split(".")[0])
        try:
            apis = self.get_gateways_sync(awsclient)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "UnrecognizedClientException":
                return []
            else:
                raise e
        deleted_ids: List[str] = []
        for api in apis:
            if "name" in api and api["name"] == self.api_name:
                if endpoints is not None and api["id"] not in endpoint_ids:
                    continue
                try:
                    awsclient.delete_rest_api(restApiId=api["id"])
                    deleted_ids.append(api["id"])
                except botocore.exceptions.ClientError as ce:
                    if ce.response["Error"]["Code"] == "TooManyRequestsException":
                        sleep(1)
                        continue
                    else:
                        if self.verbose:
                            print(f"Failed to delete API {api['id']}. Reason: {ce}")
        return deleted_ids

    @staticmethod
    def get_gateways_sync(client: Any) -> List[Dict[str, Any]]:
        gateways: List[Dict[str, Any]] = []
        position: Optional[str] = None
        while True:
            if position:
                response = client.get_rest_apis(limit=500, position=position)
            else:
                response = client.get_rest_apis(limit=500)
            gateways.extend(response["items"])
            position = response.get("position")
            if not position:
                break
        return gateways

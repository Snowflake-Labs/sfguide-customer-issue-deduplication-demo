-- Copyright 2025 Snowflake Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- ...

-- ============================================================================
-- AI_COMPLETE_BATCH - Generic Vectorized UDF for Cortex REST API (Batched)
-- Features: Token reduction via Prompt Stuffing (JSON mapping) & Sub-batching
-- ============================================================================

CREATE OR REPLACE FUNCTION AI_COMPLETE_BATCH(
    model_name VARCHAR DEFAULT 'claude-sonnet-4-5',
    system_prompt VARCHAR,
    data_prompt VARCHAR,
    response_format VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'aiohttp', 'pydantic')
HANDLER = 'complete_batch'
EXTERNAL_ACCESS_INTEGRATIONS = (cortex_loopback_integration)
SECRETS = ('cred' = cortex_auth_token)
AS $$
"""
Cache-Optimized Vectorized UDF for Cortex REST API.
Executes 1-to-1 concurrent asynchronous HTTP requests per row, ensuring strict data isolation.
Leverages Prompt Caching (ephemeral) to drastically reduce System Prompt token costs.
"""
import json
import asyncio
import aiohttp
import pandas as pd
from typing import Optional
from pydantic import BaseModel
from _snowflake import get_generic_secret_string, get_account_identifier, vectorized

_COMPLETE_API_ENDPOINT = f"https://{get_account_identifier()}.snowflakecomputing.com/api/v2/cortex/inference:complete"
_MAX_BATCH_ROWS = 20

class TokenUsage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0
    
    @classmethod
    def from_api_response(cls, usage: dict) -> "TokenUsage":
        return cls(
            input_tokens=usage.get("prompt_tokens", 0),
            output_tokens=usage.get("completion_tokens", 0),
            cache_write_tokens=usage.get("cache_creation_input_tokens", 0),
            cache_read_tokens=usage.get("cache_read_input_tokens", 0)
        )

class BatchCompletionResult(BaseModel):
    success: bool
    response: Optional[str] = None
    error: Optional[str] = None
    model: Optional[str] = None
    usage: Optional[TokenUsage] = None
    
    def to_dict(self) -> dict:
        return self.model_dump(exclude_none=True)
    
    @classmethod
    def success_result(cls, response: str, usage: dict, model: str) -> "BatchCompletionResult":
        return cls(
            success=True,
            response=response,
            model=model,
            usage=TokenUsage.from_api_response(usage)
        )
    
    @classmethod
    def error_result(cls, error: str) -> "BatchCompletionResult":
        return cls(success=False, error=error)
    
    @classmethod
    def empty_result(cls) -> "BatchCompletionResult":
        return cls(success=True, response="")

class CortexAsyncClient:
    def __init__(self, token: str):        
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    @staticmethod
    def _parse_response_format(fmt_str):
        if not fmt_str or str(fmt_str).strip() in ('None', 'nan', ''):
            return None
        try:
            return json.loads(fmt_str)
        except json.JSONDecodeError:
            return {"type": str(fmt_str)}

    @staticmethod
    def _build_system_message(model: str, sys_prompt: str) -> list:
        if not sys_prompt:
            return []
            
        if "claude" in model.lower():
            return [{
                "role": "system", 
                "content_list": [
                    {
                        "type": "text", 
                        "text": sys_prompt, 
                        "cache_control": {"type": "ephemeral"}
                    }
                ]
            }]
        else:
            return [{
                "role": "system", 
                "content": sys_prompt
            }]

    async def _fetch_cached_completion(self, session, payload, row_idx, model):
        try:
            async with session.post(_COMPLETE_API_ENDPOINT, json=payload, headers=self.headers) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    return row_idx, BatchCompletionResult.error_result(f"HTTP_{resp.status}: {error_text[:100]}")
                
                result = await resp.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                usage = result.get("usage", {})
                
                return row_idx, BatchCompletionResult.success_result(content, usage, model)
                
        except asyncio.TimeoutError:
            return row_idx, BatchCompletionResult.error_result("Request Timed Out")
        except Exception as e:
            return row_idx, BatchCompletionResult.error_result(str(e)[:200])

    async def process_dataframe(self, df):
        if df.empty:
            return []
            
        model = str(df.iloc[0, 0])
        sys_prompt = str(df.iloc[0, 1]) if pd.notna(df.iloc[0, 1]) else ""
        base_messages = self._build_system_message(model, sys_prompt)

        connector = aiohttp.TCPConnector(limit=_MAX_BATCH_ROWS)
        timeout = aiohttp.ClientTimeout(total=120)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            
            for i, row in df.iterrows():
                user_prompt = str(row[2]) if pd.notna(row[2]) else ""
                
                if not user_prompt:
                    tasks.append(asyncio.sleep(0, result=(i, BatchCompletionResult.empty_result())))
                    continue
                    
                messages = base_messages + [{"role": "user", "content": user_prompt}]
                    
                payload = {
                    "model": model,
                    "messages": messages,
                    "temperature": 0
                }
                
                if df.shape[1] > 3:
                    resp_fmt = self._parse_response_format(row[3])
                    if resp_fmt:
                        payload["response_format"] = resp_fmt
                        
                tasks.append(self._fetch_cached_completion(session, payload, i, model))
            
            results = await asyncio.gather(*tasks)
            sorted_results = [res[1].to_dict() for res in sorted(results, key=lambda x: x[0])]
            return sorted_results

@vectorized(input=pd.DataFrame, max_batch_size=_MAX_BATCH_ROWS)
def complete_batch(df):
    token = get_generic_secret_string('cred')
    
    client = CortexAsyncClient(token)
    results = asyncio.run(client.process_dataframe(df))
    
    return pd.Series(results)
$$;

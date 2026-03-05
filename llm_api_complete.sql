-- Copyright 2025 Snowflake Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- ...

-- ============================================================================
-- LLM_API_COMPLETE - Generic Vectorized UDF for Cortex REST API (Batched)
-- Features: Token reduction via Prompt Caching & Parallel requests
-- ============================================================================

-- ============================================================================
-- STEP 0: Network Rules for Cortex REST API Access
-- ============================================================================

-- 0.1 Egress rule to allow outbound calls to Cortex API
CREATE OR REPLACE NETWORK RULE snowflake_cortex_egress_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('sfpscogs-adamle-aws-3.snowflakecomputing.com');

-- 0.2 Ingress rule to allow inbound responses from Snowflake egress IPs
-- IPs retrieved from: SELECT value:"ipv4_prefix"::VARCHAR FROM TABLE(FLATTEN(INPUT => PARSE_JSON(SYSTEM$GET_SNOWFLAKE_EGRESS_IP_RANGES())));
CREATE OR REPLACE NETWORK RULE snowflake_cortex_ingress_rule
    MODE = INGRESS
    TYPE = IPV4
    VALUE_LIST = ('<your-snowflake-egress-ips>');

-- 0.3 Stored procedure to auto-update ingress rule with current Snowflake egress IPs
CREATE OR REPLACE PROCEDURE UPDATE_CORTEX_INGRESS_RULE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    ip_list VARCHAR;
    ddl_stmt VARCHAR;
BEGIN
    -- Get current valid IP ranges as comma-separated list
    SELECT LISTAGG('''' || value:"ipv4_prefix"::VARCHAR || '''', ', ') 
    INTO :ip_list
    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(SYSTEM$GET_SNOWFLAKE_EGRESS_IP_RANGES())))
    WHERE value:"expires"::TIMESTAMP > CURRENT_TIMESTAMP();
    
    -- Recreate the network rule with updated IPs
    ddl_stmt := 'CREATE OR REPLACE NETWORK RULE snowflake_cortex_ingress_rule MODE = INGRESS TYPE = IPV4 VALUE_LIST = (' || :ip_list || ')';
    EXECUTE IMMEDIATE :ddl_stmt;
    
    RETURN 'Updated ingress rule with IPs: ' || :ip_list;
END;
$$;

-- 0.4 Task to auto-update ingress rule weekly (runs every Sunday at 2 AM)
CREATE OR REPLACE TASK UPDATE_CORTEX_INGRESS_RULE_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 * * 0 America/Los_Angeles'
    COMMENT = 'Auto-updates Cortex ingress network rule with current Snowflake egress IPs'
AS
    CALL UPDATE_CORTEX_INGRESS_RULE();

-- Enable the task (must be run separately after creation)
-- ALTER TASK UPDATE_CORTEX_INGRESS_RULE_TASK RESUME;

-- ============================================================================
-- STEP 1: Create the UDF
-- ============================================================================

CREATE OR REPLACE FUNCTION LLM_API_COMPLETE(
    model_name VARCHAR,
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
from _snowflake import get_generic_secret_string, vectorized

_COMPLETE_API_ENDPOINT = "https://sfpscogs-adamle-aws-3.snowflakecomputing.com/api/v2/cortex/inference:complete"
_MAX_BATCH_ROWS = 20


class BatchCompletionResult(BaseModel):
    success: bool
    response: Optional[str] = None
    error: Optional[str] = None
    model: Optional[str] = None
    usage: Optional[dict] = None    
    
    def to_dict(self) -> dict:
        return self.model_dump(exclude_none=True)
    
    @classmethod
    def success_result(cls, response: str, usage: dict, model: str) -> "BatchCompletionResult":
        return cls(
            success=True,
            response=response,
            model=model,
            usage=usage,
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
        if pd.isna(fmt_str) or str(fmt_str).strip() in ('None', 'nan', ''):
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

    def _build_payload(self, model, base_messages, row, has_fmt_col):
        user_prompt = str(row[2]) if pd.notna(row[2]) else ""
        if not user_prompt:
            return None

        messages = base_messages + [{"role": "user", "content": user_prompt}]
        payload = {
            "model": model,
            "messages": messages,
            "temperature": 0,
            "stream": False,
        }
        if has_fmt_col:
            resp_fmt = self._parse_response_format(row[3])
            if resp_fmt:
                payload["response_format"] = resp_fmt
        return payload

    async def process_dataframe(self, df):
        if df.empty:
            return []

        model = str(df.iloc[0, 0])
        sys_prompt = str(df.iloc[0, 1]) if pd.notna(df.iloc[0, 1]) else ""
        base_messages = self._build_system_message(model, sys_prompt)
        has_fmt_col = df.shape[1] > 3

        connector = aiohttp.TCPConnector(limit=_MAX_BATCH_ROWS, ssl=False)
        timeout = aiohttp.ClientTimeout(total=120)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for i, row in df.iterrows():
                payload = self._build_payload(model, base_messages, row, has_fmt_col)
                if payload is None:
                    tasks.append(asyncio.sleep(0, result=(i, BatchCompletionResult.empty_result())))
                else:
                    tasks.append(self._fetch_cached_completion(session, payload, i, model))

            results = await asyncio.gather(*tasks)
            return [res[1].to_dict() for res in sorted(results, key=lambda x: x[0])]

@vectorized(input=pd.DataFrame, max_batch_size=_MAX_BATCH_ROWS)
def complete_batch(df):
    token = get_generic_secret_string('cred')
    
    client = CortexAsyncClient(token)
    results = asyncio.run(client.process_dataframe(df))
    
    return pd.Series(results)
$$;

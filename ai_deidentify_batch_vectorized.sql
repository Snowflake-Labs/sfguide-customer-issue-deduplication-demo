-- Copyright 2025 Snowflake Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ============================================================================
-- AI De-identification Batch Processing - Vectorized UDF with Cortex REST API
-- Architecture-A: Warehouse-Native Vectorized UDF with Prompt Caching
-- Requires: ai_deidentify.sql to be executed first for base functions
-- ============================================================================

-- ============================================================================
-- STEP 1: Security Configuration
-- Run these steps in order. Steps 1.4-1.5 require manual intervention.
-- ============================================================================

-- 1.1 Create the Network Rule (Egress to Self - Loopback)
-- IMPORTANT: Replace <your-org>-<your-account> with your actual account identifier
-- Use the format from your Snowsight URL (e.g., sfpscogs-adamle-aws-3)
CREATE OR REPLACE NETWORK RULE snowflake_cortex_egress_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('sfpscogs-adamle-aws-3.snowflakecomputing.com');

-- 1.2 Generate Programmatic Access Token (PAT) for your CURRENT user
-- IMPORTANT: Run this command and SAVE the token_secret value from the output.
-- The token is only shown ONCE and cannot be retrieved later.
ALTER USER CURRENT_USER() ADD PROGRAMMATIC ACCESS TOKEN cortex_vectorized_udf_pat;

-- 1.3 Store the Token in a Secret
-- Replace '<PASTE_YOUR_PAT_TOKEN_SECRET_HERE>' with the token_secret from step 1.2
CREATE OR REPLACE SECRET cortex_auth_token
    TYPE = GENERIC_STRING
    SECRET_STRING = '<PASTE_YOUR_PAT_TOKEN_SECRET_HERE>';

-- 1.4 Create External Access Integration
-- Binds the network rule and secret together for use by the UDF
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION cortex_loopback_integration
    ALLOWED_NETWORK_RULES = (snowflake_cortex_egress_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (cortex_auth_token)
    ENABLED = TRUE;

-- ============================================================================
-- STEP 2: Vectorized Python UDF with Prompt Caching
-- Uses @vectorized decorator to receive batches as pandas DataFrame.
-- Snowflake automatically batches rows for efficient processing.
-- ============================================================================

CREATE OR REPLACE FUNCTION LLM_EXTRACT_ENTITIES_BATCH(raw_text VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'aiohttp')
HANDLER = 'extract_entities_batch'
EXTERNAL_ACCESS_INTEGRATIONS = (cortex_loopback_integration)
SECRETS = ('cred' = cortex_auth_token)
AS $$
"""
Vectorized UDF for batch entity extraction using Cortex REST API.
Features: Dynamic batching, parallel API calls, SSE parsing, prompt caching.
"""
import json
import asyncio
import aiohttp
import pandas as pd
from _snowflake import get_generic_secret_string, vectorized

# =============================================================================
# Configuration
# =============================================================================
API_ENDPOINT = "https://sfpscogs-adamle-aws-3.snowflakecomputing.com/api/v2/cortex/inference:complete"
MODEL = "claude-sonnet-4-5"
MAX_INPUT_TOKENS = 150000
MAX_OUTPUT_TOKENS = 14000
TOKENS_PER_CHAR = 0.25
OUTPUT_TOKENS_PER_RECORD = 150

SYSTEM_PROMPT = """Extract sensitive entities from text records prefixed with [RECORD_N].

INFO_TYPES: PERSON_NAME, AUSTRALIAN_PHONE_NUMBER (10 digits, 04/02/03/07/08), 
EMAIL_ADDRESS, AUSTRALIAN_DRIVERS_LICENSE (state-specific formats), CREDIT_CARD_NUMBER (13-19 digits).

Rules: Return exact substrings only. Skip invalid patterns.
Output: {"results": [{"record_index": N, "entities": [{"info_type": "...", "value": "..."}]}]}"""

RESPONSE_SCHEMA = {"type": "json", "schema": {
    "type": "object", "required": ["results"],
    "properties": {"results": {"type": "array", "items": {
        "type": "object", "required": ["record_index", "entities"],
        "properties": {
            "record_index": {"type": "integer"},
            "entities": {"type": "array", "items": {
                "type": "object", "required": ["info_type", "value"],
                "properties": {
                    "info_type": {"type": "string", "enum": [
                        "PERSON_NAME", "AUSTRALIAN_PHONE_NUMBER", "EMAIL_ADDRESS",
                        "AUSTRALIAN_DRIVERS_LICENSE", "CREDIT_CARD_NUMBER"]},
                    "value": {"type": "string"}
                }
            }}
        }
    }}}
}}

# =============================================================================
# Core Functions
# =============================================================================
def create_batches(texts):
    """Split texts into sub-batches within token limits."""
    batches, batch = [], {"texts": [], "indices": [], "tokens": 0}
    for idx, text in enumerate(texts):
        tokens = int(len(f"[RECORD_{idx}]\n{text}\n\n") * TOKENS_PER_CHAR)
        if batch["texts"] and (batch["tokens"] + tokens > MAX_INPUT_TOKENS or
                               len(batch["texts"]) * OUTPUT_TOKENS_PER_RECORD > MAX_OUTPUT_TOKENS):
            batches.append(batch)
            batch = {"texts": [], "indices": [], "tokens": 0}
        batch["texts"].append(text)
        batch["indices"].append(idx)
        batch["tokens"] += tokens
    if batch["texts"]:
        batches.append(batch)
    return batches

async def call_api(session, texts, indices, token):
    """Call Cortex API for a batch and return mapped results."""
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content_list": [
                {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}]},
            {"role": "user", "content": "\n\n".join(f"[RECORD_{i}]\n{t}" for i, t in enumerate(texts))}
        ],
        "response_format": RESPONSE_SCHEMA,
        "max_tokens": min(MAX_OUTPUT_TOKENS, len(texts) * OUTPUT_TOKENS_PER_RECORD + 500),
        "temperature": 0
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json",
               "Accept": "application/json, text/event-stream"}
    
    try:
        async with session.post(API_ENDPOINT, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=300)) as resp:
            if resp.status != 200:
                return {"error": f"HTTP_{resp.status}", "indices": indices}
            
            content, usage = "", {}
            async for line in resp.content:
                line = line.decode('utf-8').strip()
                if line.startswith('data:'):
                    try:
                        chunk = json.loads(line[5:].strip())
                        content += chunk.get("choices", [{}])[0].get("delta", {}).get("content", "")
                        if "usage" in chunk:
                            usage = chunk["usage"]
                    except json.JSONDecodeError:
                        pass
            
            result_map = {r.get("record_index", i): r.get("entities", [])
                          for i, r in enumerate(json.loads(content).get("results", []) if content else [])}
            return {"results": {orig: result_map.get(i, []) for i, orig in enumerate(indices)}, "usage": usage}
    except Exception as e:
        return {"error": str(e)[:200], "indices": indices}

async def process_all(texts, token):
    """Process all batches in parallel."""
    batches = create_batches(texts)
    results, usage, errors = {}, {"prompt_tokens": 0, "completion_tokens": 0}, []
    
    async with aiohttp.ClientSession() as session:
        responses = await asyncio.gather(*[call_api(session, b["texts"], b["indices"], token) for b in batches],
                                         return_exceptions=True)
    for resp in responses:
        if isinstance(resp, Exception) or "error" in resp:
            errors.append(resp if isinstance(resp, dict) else {"error": str(resp)})
        else:
            results.update(resp.get("results", {}))
            for k in usage:
                usage[k] += resp.get("usage", {}).get(k, 0)
    return {"results": results, "usage": usage, "errors": errors, "num_batches": len(batches)}

# =============================================================================
# UDF Entry Point
# =============================================================================
@vectorized(input=pd.DataFrame, max_batch_size=100)
def extract_entities_batch(df):
    """Vectorized handler - process batch of rows, return entity results."""
    token = get_generic_secret_string('cred')
    texts = df.iloc[:, 0].fillna("").astype(str).tolist()
    if not texts:
        return pd.Series([{"success": True, "entities": []}] * len(df))
    
    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(process_all(texts, token))
    finally:
        loop.close()
    
    error_idx = {e.get("indices", [None])[0] for e in result["errors"] if isinstance(e, dict)}
    meta = {"num_batches": result["num_batches"], **result["usage"]}
    return pd.Series([
        {"success": False, "entities": [], "error": "BATCH_ERROR"} if i in error_idx
        else {"success": True, "entities": result["results"].get(i, []), "_meta": meta}
        for i in range(len(df))
    ])
$$;

-- ============================================================================
-- STEP 3: AI_DEIDENTIFY_TEXT - Scalar wrapper using the vectorized UDF
-- Call this on individual rows; Snowflake batches them automatically.
-- ============================================================================

CREATE OR REPLACE FUNCTION AI_DEIDENTIFY_TEXT(raw_text VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
SELECT (
    SELECT OBJECT_CONSTRUCT(
        'original_text', raw_text,
        'deidentified_text',
        REDUCE(
            cleaned_entities,
            raw_text::STRING,
            (acc, el) -> REPLACE(
                acc,
                el:value::STRING,
                '__ENTITY_' || el:type::STRING || '(' || el:token::STRING || ')__'
            )
        ),
        'extracted_entities', cleaned_entities
    )
    FROM (
        SELECT TRANSFORM(
            TRANSFORM(
                FILTER(
                    LLM_EXTRACT_ENTITIES_BATCH(raw_text):entities,
                    e -> e:value IS NOT NULL AND e:info_type IS NOT NULL
                ),
                e -> OBJECT_CONSTRUCT(
                    'type', e:info_type::STRING,
                    'value', e:value::STRING,
                    'cleaned_value', CLEAN_SENSITIVE_VALUE(e:info_type::STRING, e:value::STRING)
                )
            ),
            e -> OBJECT_INSERT(e, 'token', TOKENIZE_SENSITIVE_VALUE(e:cleaned_value::STRING))
        ) AS cleaned_entities
    )
)
$$;

-- ============================================================================
-- Example Usage:
-- ============================================================================
-- 
-- -- Process table rows - Snowflake automatically batches calls to the vectorized UDF
-- -- This leverages prompt caching across all rows in the batch
-- SELECT 
--     id,
--     raw_text,
--     AI_DEIDENTIFY_TEXT(raw_text) as result
-- FROM my_table;
--
-- -- Extract specific fields from result
-- SELECT 
--     id,
--     result:deidentified_text::STRING as deidentified_text,
--     result:extracted_entities as entities
-- FROM (
--     SELECT id, AI_DEIDENTIFY_TEXT(raw_text) as result
--     FROM my_table
-- );
--
-- -- Check cache utilization
-- SELECT 
--     LLM_EXTRACT_ENTITIES_BATCH('Test text with john@test.com'):_meta as cache_info;

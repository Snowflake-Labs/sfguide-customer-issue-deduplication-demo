# Cortex REST API Challenges and Feature Requirements

Author: [Anant Damle](mailto:anant.damle@snowflake.com)  
Last Change: 2026-02-19

## Executive Summary

This document outlines the challenges encountered when using the Cortex REST API for LLM-based AI Functions within Snowflake. While the REST API offers **cost advantages** through features like prompt caching, it introduces significant security and operational complexity. This document proposes a long-term solution requiring internal endpoint availability for Snowflake's trusted compute platforms.

## Current Solution Architecture

## Current Architecture Challenge

Building custom AI Functions that leverage advanced Cortex REST API features requires Python UDFs calling the API externally via HTTP clients. This approach is necessary to achieve:

- **Prompt caching** for significant cost reduction (up to 90% savings on cached tokens)  
- **Parallel API calls** for performance optimization  
- **Custom batching logic** based on token limits  
- **Streaming responses** for large outputs

However, this architecture introduces significant operational and security challenges.

## Need for using REST API over SQL Functions

### Cost Optimization vs. Complexity Trade-off

### REST API Cost Advantages

The Cortex REST API provides features that can **significantly reduce costs** compared to SQL-based functions:

| Feature | REST API | SQL `TRY_COMPLETE` | Cost Impact |
| :---- | :---- | :---- | :---- |
| Prompt Caching | ✅ `cache_control` parameter | ❌ Not available | Up to 90% reduction on cached system prompts |
| Batch Processing | ✅ Multiple records per call | ❌ One call per row | Reduced overhead per record |
| Token Usage Visibility | ✅ Usage metadata returned | ✅ Available via `<options>` argument | Better cost monitoring and optimization |
| Structured Outputs | ✅ `response_format` parameter | ✅ `response_format` option | JSON schema validation |

### Pricing Reference (from [Snowflake Credit Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf))

| Model | Credits per Million Tokens (AI\_COMPLETE) | US$ per Million Tokens (SQL) $4/credit | US$ per Million Tokens (REST API) |
| :---- | :---- | :---- | :---- |
| claude-4-opus | 7.50/37.50 | 30/150 | 15/75  cached: 1.5/18.75 |
| llama-3.1-405b | 1.20/1.20 | 4.8/4.8 | 2.40/2.40 |
| openai-gpt-4.1 | 1.00/4.00 | 4.00/16.00 | 2.20/8.80  cached: 0.55/- |
| llama3.1-70b | 0.36/0.36 | 1.44/1.44 | 0.72/0.72 |
| openai-gpt-5-mini | 0.14/1.10 | 0.56/4.40 | 0.28/2.20  cached: 0.03/- |
| mistral-7b | 0.08/0.10 | 0.32/0.40 | 0.15/0.20 |

**Key Insight:** With prompt caching, a system prompt that processes thousands of records only counts tokens once for the cached portion, rather than billing for the full prompt on every row. This makes the REST API **cheaper** for batch workloads, but requires complex infrastructure to access.

## Current Workarounds and Mitigations

Organizations seeking to leverage Cortex REST API cost benefits have two primary approaches:

### Workaround 1: External Access Integration with Service Account

Create a Python UDF that calls the Cortex REST API externally using the External Access Integration framework:

```py
CREATE OR REPLACE FUNCTION ai_process_with_caching(input_text VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('aiohttp', 'snowflake-snowpark-python')
EXTERNAL_ACCESS_INTEGRATIONS = (cortex_loopback_integration)
SECRETS = ('creds' = cortex_auth_token)
HANDLER = 'process'
AS $$
import aiohttp
import _snowflake

async def process(input_text):
    token = _snowflake.get_generic_secret_string('creds')
    async with aiohttp.ClientSession() as session:
        response = await session.post(
            'https://<org>-<account>.snowflakecomputing.com/api/v2/cortex/inference:complete',
            headers={'Authorization': f'Bearer {token}'},
            json={
                'model': 'claude-sonnet-4-5',
                'messages': [
                    {'role': 'system', 'content': [
                        {'type': 'text', 'text': 'System prompt here...', 'cache_control': {'type': 'ephemeral'}}
                    ]},
                    {'role': 'user', 'content': input_text}
                ]
            }
        )
        return await response.json()
$$;
```

**Pros:** Full REST API feature access including prompt caching **Cons:** Requires network policy bypass for service accounts (see Challenge 1\)

### Workaround 2: Array-Based SQL Batching with TRY\_COMPLETE

Use `SNOWFLAKE.CORTEX.TRY_COMPLETE` with array inputs to batch process multiple records without external access:

```sql
WITH source_batch AS (
    SELECT 
        ARRAY_AGG(OBJECT_CONSTRUCT('role', 'user', 'content', input_text)) 
            WITHIN GROUP (ORDER BY id) AS messages,
        ARRAY_AGG(id) WITHIN GROUP (ORDER BY id) AS ids
    FROM my_table
    WHERE batch_id = 1
),
results AS (
    SELECT 
        SNOWFLAKE.CORTEX.TRY_COMPLETE(
            'claude-sonnet-4-5',
            messages,
            {'max_tokens': 4096}
        ) AS batch_result,
        ids
    FROM source_batch
)
SELECT 
    ids[r.index]::INT AS id,
    r.value:messages::STRING AS result
FROM results, LATERAL FLATTEN(input => batch_result:choices) r;
```

**Pros:** No external access required, processes multiple records in batched SQL pattern **Cons:** No prompt caching, complex result mapping, error-prone array handling (see Challenge 2\)

## Challenge 1: Network and Security Complexity

### Required Security Objects

The REST API approach (Workaround 1\) requires creating and maintaining:

```sql
-- 1. Network egress rule (account-specific URL)
CREATE NETWORK RULE snowflake_cortex_egress_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('<org>-<account>.snowflakecomputing.com');

-- 2. Programmatic Access Token (PAT)
ALTER USER <user> ADD PROGRAMMATIC ACCESS TOKEN cortex_ai_function_pat;

-- 3. Secret storage for token
CREATE SECRET cortex_auth_token
    TYPE = GENERIC_STRING
    SECRET_STRING = '<PAT_TOKEN>';

-- 4. External access integration
CREATE EXTERNAL ACCESS INTEGRATION cortex_loopback_integration
    ALLOWED_NETWORK_RULES = (snowflake_cortex_egress_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (cortex_auth_token)
    ENABLED = TRUE;
```

### Network Policy Bypass Concern

A critical security risk emerges when customers have network policies restricting egress and ingress traffic. The mitigation strategy requires:

1. **Creating a dedicated service user** with elevated privileges  
2. **Bypassing network policy constraints** for this user to allow Cortex API calls  
3. **Storing PATs** that grant API access outside normal session controls  
4. The service account requires an allowlisting for incoming requests **bypassing ingress rules**

**Security Risks:**

- Service accounts with network policy exemptions create audit and compliance gaps  
- PAT tokens require manual rotation and secure storage  
- Exempted users represent potential lateral movement vectors  
- Difficult to enforce least-privilege principles

## Challenge 2: Limitations of SQL TRY\_COMPLETE

While `TRY_COMPLETE` provides token visibility and structured outputs, it lacks key features for cost optimization:

| Issue | Description |
| :---- | :---- |
| No Prompt Caching | Cannot leverage `cache_control` parameter available in REST API |
| One Call Per Row | Each row in a SELECT executes a separate LLM call |
| No Parallel Processing | Cannot batch multiple prompts in a single API call |
| Error Handling | `TRY_COMPLETE` returns NULL on failure without detailed error information |

**Example \- Processing Multiple Rows:**

```sql
-- Each row triggers a separate LLM call with full token billing
SELECT 
    id,
    SNOWFLAKE.CORTEX.TRY_COMPLETE(
        'claude-sonnet-4-5',
        [
            {'role': 'system', 'content': 'Extract entities from the text.'},
            {'role': 'user', 'content': input_text}
        ],
        {'max_tokens': 500}
    ):usage:total_tokens::INT AS tokens_used
FROM my_table;
```

With 10,000 rows and a 100-token system prompt, this bills for 1,000,000 system prompt tokens. REST API prompt caching would bill once for the cached portion.

## Proposed Long-Term Solution

### Internal Cortex REST API Endpoint

Snowflake should provide an **internal endpoint** for the Cortex REST API accessible from trusted compute platforms without external network egress.

**Target Platforms:**

- Snowpark Container Services (SPCS)  
- Snowflake Notebooks  
- Warehouse-based UDFs/UDTFs

**Proposed Architecture:**

**Benefits:**

| Benefit | Description |
| :---- | :---- |
| No Network Egress | Eliminates external access integration requirements |
| Unified Pricing | Credits consumed same as SQL-based Cortex functions |
| Session Authentication | Uses existing session context, no PAT management |
| Full REST API Features | Prompt caching, streaming, structured outputs |
| Security Compliance | No network policy exemptions required |
| Simplified Development | Standard HTTP calls without security object setup |
| Cost Optimization | Access to prompt caching without security overhead |

### Recommended Implementation

1. **Internal DNS resolution** for `cortex.internal.snowflake.com` within trusted compute  
2. **Session token passthrough** using existing Snowflake authentication context  
3. **Credit-based billing** aligned with SQL function consumption model  
4. **Feature parity** with external REST API including:  
   - Prompt caching (`cache_control`)  
   - Structured outputs (`response_format`)  
   - Streaming responses  
   - Multi-modal inputs

## Summary of Recommendations

| Priority | Recommendation | Impact |
| :---- | :---- | :---- |
| **High** | Internal Cortex REST endpoint for trusted compute | Eliminates security overhead while preserving cost optimization features |
| **Medium** | Vectorized batch processing in SQL `TRY_COMPLETE` function | Reduces need for REST API workarounds |
| **Low** | Prompt caching support in SQL `TRY_COMPLETE` function | Reduces need for REST API workarounds |

## References

- [Snowflake Credit Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)  
- [TRY\_COMPLETE (SNOWFLAKE.CORTEX) Documentation](https://docs.snowflake.com/en/sql-reference/functions/try_complete-snowflake-cortex)  
- [COMPLETE (SNOWFLAKE.CORTEX) Documentation](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex)  
- [Cortex LLM Functions Overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)  
- [External Access Integration Documentation](https://docs.snowflake.com/en/developer-guide/external-network-access/external-network-access-overview)  
- [Network Policy Documentation](https://docs.snowflake.com/en/user-guide/network-policies)  
- [Cortex REST API Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-llm-rest-api)


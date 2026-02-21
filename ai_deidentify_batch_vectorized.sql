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
-- AI De-identification Batch Processing - Using AI_COMPLETE_BATCH
-- Requires: ai_complete_batch.sql to be executed first for the base UDF
-- Requires: ai_deidentify.sql to be executed first for helper functions
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
    VALUE_LIST = ('<your-org>-<your-account>.snowflakecomputing.com');

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
-- STEP 2: LLM_EXTRACT_ENTITIES_BATCH - Wrapper using AI_COMPLETE_BATCH
-- ============================================================================

CREATE OR REPLACE FUNCTION LLM_EXTRACT_ENTITIES_BATCH(raw_text VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
SELECT 
    CASE 
        WHEN result:success = TRUE THEN
            OBJECT_CONSTRUCT(
                'success', TRUE,
                'entities', TRY_PARSE_JSON(result:response)
            )
        ELSE
            OBJECT_CONSTRUCT(
                'success', FALSE,
                'entities', [],
                'error', result:error
            )
    END
FROM (
    SELECT AI_COMPLETE_BATCH(
        'claude-sonnet-4-5',
        -- system prompt
        'Extract sensitive entities from the text.

INFO_TYPES: PERSON_NAME, AUSTRALIAN_PHONE_NUMBER (10 digits, 04/02/03/07/08), 
EMAIL_ADDRESS, AUSTRALIAN_DRIVERS_LICENSE (state-specific formats), CREDIT_CARD_NUMBER (13-19 digits).

Rules: Return exact substrings only. Skip invalid patterns.
Output JSON array: [{"info_type": "...", "value": "..."}]',
        -- user prompt
        raw_text,
        '{"type": "json", "schema": {"type": "array", "items": {"type": "object", "required": ["info_type", "value"], "properties": {"info_type": {"type": "string", "enum": ["PERSON_NAME", "AUSTRALIAN_PHONE_NUMBER", "EMAIL_ADDRESS", "AUSTRALIAN_DRIVERS_LICENSE", "CREDIT_CARD_NUMBER"]}, "value": {"type": "string"}}}}}'
    ) AS result
)
$$;

-- ============================================================================
-- STEP 3: AI_DEIDENTIFY_TEXT - Scalar wrapper using the entity extraction
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
-- -- Test entity extraction directly
-- SELECT LLM_EXTRACT_ENTITIES_BATCH('Contact John Smith at john@example.com or 0412345678');

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
-- AI De-identification Batch Processing
-- Uses vectorized UDF (LLM_EXTRACT_ENTITIES_BATCH) from ai_deidentify_batch_vectorized.sql
-- Requires: 
--   1. ai_deidentify.sql for base functions (CLEAN_SENSITIVE_VALUE, TOKENIZE_SENSITIVE_VALUE)
--   2. ai_deidentify_batch_vectorized.sql for LLM_EXTRACT_ENTITIES_BATCH vectorized UDF
-- ============================================================================

-- ============================================================================
-- AI_DEIDENTIFY_TEXT_BATCH
-- Batch version of DEIDENTIFY_TEXT - processes multiple texts using vectorized UDF
-- Input: Array of raw text strings
-- Output: Array of objects, each containing deidentified_text and extracted_entities
-- ============================================================================

CREATE OR REPLACE FUNCTION AI_DEIDENTIFY_TEXT_BATCH(raw_texts ARRAY)
RETURNS ARRAY
LANGUAGE SQL
AS
$$
WITH extracted AS (
    SELECT 
        f.index as idx,
        f.value::STRING as raw_text,
        LLM_EXTRACT_ENTITIES_BATCH(f.value::STRING) as extraction_result
    FROM TABLE(FLATTEN(raw_texts)) f
),
with_entities AS (
    SELECT 
        idx,
        raw_text,
        extraction_result,
        TRANSFORM(
            TRANSFORM(
                FILTER(
                    extraction_result:entities,
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
    FROM extracted
)
SELECT ARRAY_AGG(
    OBJECT_CONSTRUCT(
        'record_index', idx,
        'original_text', raw_text,
        'success', extraction_result:success::BOOLEAN,
        'deidentified_text',
        CASE 
            WHEN extraction_result:success::BOOLEAN THEN
                REDUCE(
                    cleaned_entities,
                    raw_text,
                    (acc, el) -> REPLACE(
                        acc,
                        el:value::STRING,
                        '__ENTITY_' || el:type::STRING || '(' || el:token::STRING || ')__'
                    )
                )
            ELSE raw_text
        END,
        'extracted_entities', cleaned_entities,
        '_meta', extraction_result:_meta
    )
) WITHIN GROUP (ORDER BY idx)
FROM with_entities
$$;

-- ============================================================================
-- AI_DEIDENTIFY_TEXT - Scalar wrapper (convenience function)
-- Processes a single text using the vectorized UDF
-- Snowflake automatically batches multiple scalar calls for efficiency
-- ============================================================================

CREATE OR REPLACE FUNCTION AI_DEIDENTIFY_TEXT(raw_text VARCHAR)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
WITH extracted AS (
    SELECT LLM_EXTRACT_ENTITIES_BATCH(raw_text) as extraction_result
),
with_entities AS (
    SELECT 
        extraction_result,
        TRANSFORM(
            TRANSFORM(
                FILTER(
                    extraction_result:entities,
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
    FROM extracted
)
SELECT OBJECT_CONSTRUCT(
    'original_text', raw_text,
    'success', extraction_result:success::BOOLEAN,
    'deidentified_text',
    CASE 
        WHEN extraction_result:success::BOOLEAN THEN
            REDUCE(
                cleaned_entities,
                raw_text::STRING,
                (acc, el) -> REPLACE(
                    acc,
                    el:value::STRING,
                    '__ENTITY_' || el:type::STRING || '(' || el:token::STRING || ')__'
                )
            )
        ELSE raw_text
    END,
    'extracted_entities', cleaned_entities,
    '_meta', extraction_result:_meta
)
FROM with_entities
$$;

-- ============================================================================
-- Example Usage:
-- ============================================================================
-- 
-- -- Process single text (Snowflake auto-batches when used on multiple rows)
-- SELECT AI_DEIDENTIFY_TEXT('Contact John Smith at john@email.com or 0412 345 678');
--
-- -- Extract fields from scalar result
-- SELECT 
--     result:success::BOOLEAN as success,
--     result:deidentified_text::STRING as deidentified,
--     result:extracted_entities as entities
-- FROM (
--     SELECT AI_DEIDENTIFY_TEXT('Contact John Smith at john@email.com') as result
-- );
--
-- -- Process multiple texts in one call
-- SELECT AI_DEIDENTIFY_TEXT_BATCH(
--     ARRAY_CONSTRUCT(
--         'Contact John Smith at john@email.com or 0412 345 678',
--         'Sarah Connor, license NSW12345678, called about order'
--     )
-- );
--
-- -- Process table data efficiently (vectorized UDF handles batching automatically)
-- SELECT 
--     id,
--     raw_text,
--     AI_DEIDENTIFY_TEXT(raw_text) as result
-- FROM my_table;
--
-- -- Extract specific fields from table
-- SELECT 
--     id,
--     result:deidentified_text::STRING as deidentified_text,
--     result:extracted_entities as entities
-- FROM (
--     SELECT id, AI_DEIDENTIFY_TEXT(raw_text) as result
--     FROM my_table
-- );

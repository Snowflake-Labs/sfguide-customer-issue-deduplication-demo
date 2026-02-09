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
-- Batch versions of entity extraction and de-identification functions
-- Requires: ai_deidentify.sql to be executed first for base functions
-- ============================================================================

-- ============================================================================
-- LLM_EXTRACT_ENTITIES_BATCH
-- Extracts entities from multiple texts in a single LLM call
-- ============================================================================

CREATE OR REPLACE FUNCTION LLM_EXTRACT_ENTITIES_BATCH(raw_texts ARRAY)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
SNOWFLAKE.CORTEX.TRY_COMPLETE(
    'claude-sonnet-4-5',
    [
        {
            'role': 'system',
            'content': '
            Act as a sensitive data extraction system. You will receive MULTIPLE text records to process in a single batch.
            Each record is prefixed with [RECORD_N] where N is the record index (0-based).
            
            For EACH record, extract all instances of the following Information Types (INFO_TYPES):
            -   PERSON_NAME
            -   AUSTRALIAN_PHONE_NUMBER
            -   EMAIL_ADDRESS
            -   AUSTRALIAN_DRIVERS_LICENSE
            -   CREDIT_CARD_NUMBER

            ---
            VALIDATION RULES (only extract if valid):

            AUSTRALIAN_PHONE_NUMBER - Must satisfy ALL:
              * Contains 10 digits total (excluding country code +61)
              * Mobile numbers start with 04 (or +614)
              * Landline numbers start with 02, 03, 07, or 08 (area codes)
              * DO NOT flag: numbers with fewer than 10 digits, numbers starting with invalid prefixes, order IDs, ticket numbers, or reference codes

            CREDIT_CARD_NUMBER - Must satisfy ALL:
              * Contains exactly 13-19 digits
              * Starts with valid prefix: 4 (Visa), 5 (Mastercard), 3 (Amex/Diners), 6 (Discover)
              * DO NOT flag: numbers that are too short/long, sequential numbers like 1234567890, or obviously fake patterns like 0000-0000-0000-0000

            EMAIL_ADDRESS - Must satisfy ALL:
              * Contains exactly one @ symbol
              * Has valid format: local-part@domain.tld
              * Domain has at least one dot with valid TLD
              * DO NOT flag: partial emails, @mentions without domain, or malformed addresses like "user@" or "@domain.com"

            AUSTRALIAN_DRIVERS_LICENSE - Must match ONE of these state formats:
              * NSW (New South Wales): 6-8 alphanumeric characters
              * VIC (Victoria): 9-10 digits only
              * QLD (Queensland): 8-9 digits only
              * SA (South Australia): 6 alphanumeric characters
              * WA (Western Australia): 7 digits only
              * TAS (Tasmania): 6-8 alphanumeric characters
              * NT (Northern Territory): 6-10 digits only
              * ACT (Australian Capital Territory): 10 digits only
              * May be prefixed with state code (e.g., "NSW12345678", "VIC-123456789")
              * DO NOT flag: short numeric codes, order/reference numbers, or IDs that dont match above patterns

            ---
            OUTPUT CONSTRAINTS:
            * Return a "results" array where each element corresponds to the input record at that index
            * Each result contains an "entities" array for that specific record
            * DO NOT EXTRACT URLs or invalid data
            * The value MUST be the exact substring from the original text
            * DO NOT normalize or reformat values
            '
        },
        {
            'role': 'user',
            'content': (
                SELECT LISTAGG('[RECORD_' || idx || ']\n' || val || '\n\n', '')
                FROM (SELECT INDEX as idx, VALUE::STRING as val FROM TABLE(FLATTEN(raw_texts)))
            )
        }
    ],
    {         
        'max_tokens': 16384,
        'response_format': {
            'type': 'json',
            'schema': {
                'type': 'object',
                'properties': {
                    'results': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'record_index': {
                                    'type': 'integer'
                                },
                                'entities': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'object',
                                        'properties': {
                                            'info_type': {
                                                'type': 'string',
                                                'enum': ['PERSON_NAME', 'AUSTRALIAN_PHONE_NUMBER', 'EMAIL_ADDRESS', 'AUSTRALIAN_DRIVERS_LICENSE', 'CREDIT_CARD_NUMBER']
                                            },
                                            'value': {
                                                'type': 'string'
                                            }
                                        },
                                        'required': ['info_type', 'value']
                                    }
                                }
                            },
                            'required': ['record_index', 'entities']
                        }
                    }
                },
                'required': ['results']
            }
        }
    }
)
$$;

-- ============================================================================
-- DEIDENTIFY_TEXT_BATCH
-- Batch version of DEIDENTIFY_TEXT - processes multiple texts in one LLM call
-- Input: Array of raw text strings
-- Output: Array of objects, each containing deidentified_text and extracted_entities
-- Requires: CLEAN_SENSITIVE_VALUE and TOKENIZE_VALUE from ai_deidentify.sql
-- ============================================================================

CREATE OR REPLACE FUNCTION DEIDENTIFY_TEXT_BATCH(raw_texts ARRAY)
RETURNS ARRAY
LANGUAGE SQL
AS
$$
SELECT TRANSFORM(
    LLM_EXTRACT_ENTITIES_BATCH(raw_texts):structured_output[0]:raw_message:results,
    r -> (
        SELECT OBJECT_CONSTRUCT(
            'record_index', r:record_index::INT,
            'original_text', raw_texts[r:record_index::INT],
            'deidentified_text',
            REDUCE(
                cleaned_entities,
                raw_texts[r:record_index::INT]::STRING,
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
                        r:entities,
                        e -> e:value IS NOT NULL AND e:info_type IS NOT NULL
                    ),
                    e -> OBJECT_CONSTRUCT(
                        'type', e:info_type::STRING,
                        'value', e:value::STRING,
                        'cleaned_value', CLEAN_SENSITIVE_VALUE(e:info_type::STRING, e:value::STRING)
                    )
                ),
                e -> OBJECT_INSERT(e, 'token', TOKENIZE_VALUE(e:cleaned_value::STRING))
            ) AS cleaned_entities
        )
    )
)
$$;

-- ============================================================================
-- Example Usage:
-- ============================================================================
-- 
-- -- Process multiple texts in one LLM call
-- SELECT DEIDENTIFY_TEXT_BATCH(
--     ARRAY_CONSTRUCT(
--         'Contact John Smith at john@email.com or 0412 345 678',
--         'Sarah Connor, license NSW12345678, called about order',
--         'Technical issue - check https://api.example.com for logs'
--     )
-- );
--
-- -- Flatten batch results for analysis
-- SELECT 
--     r.value:record_index::INT as record_index,
--     r.value:original_text::STRING as original_text,
--     r.value:deidentified_text::STRING as deidentified_text,
--     r.value:extracted_entities as entities
-- FROM TABLE(FLATTEN(
--     DEIDENTIFY_TEXT_BATCH(
--         ARRAY_CONSTRUCT(
--             'Call Jane Doe at 0412-999-888',
--             'Email support@company.com for help'
--         )
--     )
-- )) r;
--
-- -- Process table data in batches (recommended batch size: 5-15 records)
-- WITH batched AS (
--     SELECT 
--         FLOOR((ROW_NUMBER() OVER (ORDER BY id) - 1) / 10) as batch_id,
--         ARRAY_AGG(raw_text) WITHIN GROUP (ORDER BY id) as texts,
--         ARRAY_AGG(id) WITHIN GROUP (ORDER BY id) as ids
--     FROM my_table
--     GROUP BY batch_id
-- )
-- SELECT 
--     b.ids[r.value:record_index::INT] as original_id,
--     r.value:deidentified_text::STRING as deidentified_text,
--     r.value:extracted_entities as entities
-- FROM batched b,
-- TABLE(FLATTEN(DEIDENTIFY_TEXT_BATCH(b.texts))) r;

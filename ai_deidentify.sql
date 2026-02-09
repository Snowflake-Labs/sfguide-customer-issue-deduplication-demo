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

CREATE OR REPLACE FUNCTION LLM_EXTRACT_ENTITIES(raw_text TEXT)
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
            Act as a sensitive data extraction system. Your task is to analyze the provided RAW_TEXT 
            and extract all instances of the following and ONLY the following Information Types (INFO_TYPES)

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
              * NSW (New South Wales): 6-8 alphanumeric characters (e.g., "AB123456", "1234567")
              * VIC (Victoria): 9-10 digits only (e.g., "123456789", "0123456789")
              * QLD (Queensland): 8-9 digits only (e.g., "12345678", "123456789")
              * SA (South Australia): 6 alphanumeric characters, typically starting with letter (e.g., "S12345", "AB1234")
              * WA (Western Australia): 7 digits only (e.g., "1234567")
              * TAS (Tasmania): 6-8 alphanumeric characters (e.g., "A12345", "AB123456")
              * NT (Northern Territory): 6-10 digits only (e.g., "123456", "1234567890")
              * ACT (Australian Capital Territory): 10 digits only (e.g., "1234567890")
              * May be prefixed with state code (e.g., "NSW12345678", "VIC-123456789", "QLD 12345678")
              * DO NOT flag: short numeric codes, order/reference numbers, or IDs that dont match above patterns

            ---
            OUTPUT CONSTRAINTS:
            * DO NOT EXTRACT:
              * ANY URLs (starting with http, https, ftp, ftps)
              * Invalid phone numbers, credit cards, or emails that fail validation rules above
            * If there is no sensitive information found, return empty entities array
            * IMPORTANT: The value in JSON output MUST be the exact substring from the raw_text
            * DO NOT normalize or reformat the value

            ---
            Here are some examples:

            Example 1 - RAW_TEXT: "Hi, my name is John Smith and you can reach me at john.smith@email.com or call 0412 345 678."
            Returns entities: PERSON_NAME "John Smith", EMAIL_ADDRESS "john.smith@email.com", AUSTRALIAN_PHONE_NUMBER "0412 345 678"

            Example 2 - RAW_TEXT: "The system logged error at https://api.example.com/v1/users endpoint."
            Returns: empty entities (URLs are not extracted)

            Example 3 - RAW_TEXT: "Reference number 12345, ticket ID 9876543, contact code 555-1234. Email us at support@ for help."
            Returns: empty entities (invalid data)
            '
        },
        {
            'role': 'user',
            'content': CONCAT('RAW_TEXT:\n', raw_text)
        }
    ],
    {         
        'max_tokens': 8192,
        'response_format': {
            'type': 'json',
            'schema': {
                'type': 'object',
                'properties': {
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
                'required': ['entities']
            }
        }
    }
)
$$;


CREATE OR REPLACE FUNCTION CLEAN_SENSITIVE_VALUE(i_type STRING, i_text STRING)
RETURNS STRING
LANGUAGE SQL
AS 
$$
CASE i_type
  WHEN 'AUSTRALIAN_PHONE_NUMBER'
    THEN REGEXP_REPLACE(
      REGEXP_REPLACE(i_text, '^\\+61', '0'),
      '[^0-9]', ''
    )  
  WHEN 'AUSTRALIAN_DRIVERS_LICENSE'
    THEN UPPER(REGEXP_REPLACE(i_text, '[^0-9A-Za-z]', ''))
  WHEN 'LICENSE_PLATE'
    THEN UPPER(REGEXP_REPLACE(i_text, '[^0-9A-Za-z]', ''))
  WHEN 'EMAIL_ADDRESS'
    THEN LOWER(TRIM(i_text))
  WHEN 'CREDIT_CARD_NUMBER'
    THEN REGEXP_REPLACE(i_text, '[^0-9]', '')
  WHEN 'PERSON_NAME'
    THEN UPPER(TRIM(REGEXP_REPLACE(i_text, '\\s+', ' ')))
  ELSE i_text
END
$$;


-- update the implementation to use your choice of tokenization
CREATE OR REPLACE FUNCTION TOKENIZE_SENSITIVE_VALUE(SENSITIVE_VALUE STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
SHA2(SENSITIVE_VALUE, 256)
$$;


-- ============================================================================
-- AI_DEIDENTIFY_TEXT
-- Main de-identification function that orchestrates the full pipeline:
--   1. Extract sensitive entities using LLM (LLM_EXTRACT_ENTITIES)
--   2. Filter out any null/invalid extractions  
--   3. Clean/normalize values for consistent matching (CLEAN_SENSITIVE_VALUE)
--   4. Generate deterministic tokens via SHA256 hash (TOKENIZE_SENSITIVE_VALUE)
--   5. Replace original sensitive values with redacted tokens in text
--
-- Input:  raw_text (TEXT) - The text containing potential sensitive information
-- Output: OBJECT with two keys:
--   - deidentified_text: Original text with sensitive values replaced by tokens
--                        Format: __ENTITY_<TYPE>(<SHA256_TOKEN>)__
--   - extracted_entities: Array of extracted entity objects containing:
--                         type, value, cleaned_value, token
--
-- Example:
--   Input:  'Contact John at john@email.com'
--   Output: {
--     "deidentified_text": "Contact __ENTITY_PERSON_NAME(abc123...)__ at __ENTITY_EMAIL_ADDRESS(def456...)__",
--     "extracted_entities": [
--       {"type": "PERSON_NAME", "value": "John", "cleaned_value": "JOHN", "token": "abc123..."},
--       {"type": "EMAIL_ADDRESS", "value": "john@email.com", "cleaned_value": "john@email.com", "token": "def456..."}
--     ]
--   }
-- ============================================================================
CREATE OR REPLACE FUNCTION AI_DEIDENTIFY_TEXT(raw_text TEXT)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
-- Step 5: Build final output object with redacted text and entity metadata
SELECT OBJECT_CONSTRUCT(
    'deidentified_text',
    -- Step 5a: REDUCE iterates through entities, replacing each sensitive value
    -- with its tokenized placeholder in the accumulated text
    REDUCE(
        cleaned_entities,
        raw_text,
        (acc, el) -> REPLACE(
            acc,
            el:value::STRING,
            '__ENTITY_' || el:type::STRING || '(' || el:token::STRING || ')__'
        )
    ),
    'extracted_entities', cleaned_entities
)
FROM (
    -- Step 4: Add SHA256 token to each entity using the cleaned value
    SELECT TRANSFORM(
        -- Step 3: Clean/normalize values and restructure entity objects
        TRANSFORM(
            -- Step 2: Filter out any null or invalid extractions
            FILTER(
                -- Step 1: Extract entities from text using LLM
                LLM_EXTRACT_ENTITIES(raw_text):structured_output[0]:raw_message:entities,
                e -> e:value IS NOT NULL AND e:info_type IS NOT NULL
            ),
            -- Step 3a: Build cleaned entity object with normalized value
            e -> OBJECT_CONSTRUCT(
                'type', e:info_type::STRING,
                'value', e:value::STRING,
                'cleaned_value', CLEAN_SENSITIVE_VALUE(e:info_type::STRING, e:value::STRING)
            )
        ),
        -- Step 4a: Append token field using already-computed cleaned_value
        e -> OBJECT_INSERT(e, 'token', TOKENIZE_SENSITIVE_VALUE(e:cleaned_value::STRING))
    ) AS cleaned_entities
)
$$;
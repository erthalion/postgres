/* src/test/modules/toyam/toyam--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION toytable" to load this file. \quit

CREATE FUNCTION toytableam_handler(internal)
RETURNS pg_catalog.table_am_handler STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD toytable TYPE TABLE HANDLER toytableam_handler



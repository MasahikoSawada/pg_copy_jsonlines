/* contrib/copy_json_lines/copy_json_lines--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION copy_json_lines" to load this file. \quit

CREATE FUNCTION jsonlines(internal)
RETURNS copy_handler
AS 'MODULE_PATHNAME', 'jsonlines_handler'
LANGUAGE C;

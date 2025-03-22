# Copyright (c) 2022-2025, PostgreSQL Global Development Group

copy_jsonlines_sources = files(
  'copy_jsonlines.c',
)

if host_system == 'windows'
  copy_jsonlines_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'copy_jsonlines',
    '--FILEDESC', 'copy_jsonlines - JSON Lines text format support for COPY command',])
endif

copy_jsonlines = shared_module('copy_jsonlines',
  copy_jsonlines_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += copy_jsonlines

install_data(
  'copy_jsonlines--1.0.sql',
  'copy_jsonlines.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'copy_jsonlines',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'copy_jsonlines',
    ],
  },
}

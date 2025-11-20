# MANIFEST — Version Inventory / Local Data Workbench

## Назначение

Проект **Version Inventory / Local Data Workbench** — это полностью локальный инструмент (без backend),
который позволяет:

- управлять множеством Excel/CSV/JSON/YAML файлов;
- выполнять быстрый глобальный поиск по данным;
- делать zero‑code аналитику (фильтры, группировки, join);
- генерировать команды и скрипты (curl/bash/sql/ssh/zabbix/ansible);
- вести инвентарь окружений, хостов, сервисов, эндпоинтов и версий;
- интегрироваться с мониторингом (Zabbix и др.) через подготовку конфигов.

## Ключевые принципы

- **Zero Backend** — весь функционал в браузере.
- **Только HTML/CSS/vanilla JS + IndexedDB + WebWorkers**.
- **Производительность** — работа с десятками мегабайт данных без зависаний.
- **Прозрачная архитектура** — модули разделены по зонам ответственности.
- **Строгий контроль качества кода** — рефакторинг и тесты обязательны.

## Где искать подробности

- Общая архитектура: `docs/architecture_overview.md`
- Спецификация проекта: `docs/project_spec.md`
- Манифест разработчика: `docs/developer_manifest.md`
- Системный промпт для Cursor: `docs/cursor_master_prompt.txt`
- Workflow промптов: `docs/prompt_workflow_chain.md`
- Манифест тестирования: `docs/test_validation_manifest.md`
- Пайплайн релизов: `docs/release_pipeline_manifest.md`
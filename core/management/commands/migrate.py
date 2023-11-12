import sys
import time
from importlib import import_module

from django.apps import apps
from django.core.management.base import BaseCommand, CommandError, no_translations
from django.core.management.sql import emit_post_migrate_signal, emit_pre_migrate_signal
from django.db import DEFAULT_DB_ALIAS, connections, router
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import AmbiguityError
from django.db.migrations.state import ModelState, ProjectState
from django.utils.module_loading import module_has_submodule
from django.utils.text import Truncator


class Command(BaseCommand):
    help = (
        "Updates database schema. Manages both apps with migrations and those without."
    )
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-checks",
            action="store_true",
            help="Skip system checks.",
        )
        parser.add_argument(
            "app_label",
            nargs="?",
            help="App label of an application to synchronize the state.",
        )
        parser.add_argument(
            "migration_name",
            nargs="?",
            help="Database state will be brought to the state after that "
            'migration. Use the name "zero" to unapply all migrations.',
        )
        parser.add_argument(
            "--noinput",
            "--no-input",
            action="store_false",
            dest="interactive",
            help="Tells Django to NOT prompt the user for input of any kind.",
        )
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help=(
                'Nominates a database to synchronize. Defaults to the "default" '
                "database."
            ),
        )
        # 只生成 migration 记录（django_migrations 表中），不生成表
        parser.add_argument(
            "--fake",
            action="store_true",
            help="Mark migrations as run without actually running them.",
        )
        # 如果执行 migrate 时，表已经存在，则可以加上 --fake-initial
        parser.add_argument(
            "--fake-initial",
            action="store_true",
            help=(
                "Detect if tables already exist and fake-apply initial migrations if "
                "so. Make sure that the current database schema matches your initial "
                "migration before using this flag. Django will only check for an "
                "existing table name."
            ),
        )
        # 打印迁移计划，也不会去修改数据库
        parser.add_argument(
            "--plan",
            action="store_true",
            help="Shows a list of the migration actions that will be performed.",
        )
        # 为没有 migration 的应用创建表（允许在没有 migration 的情况下为应用创建表。虽然不推荐这样做，但在有数百个模型的大型项目中，迁移框架有时太慢。）
        parser.add_argument(
            "--run-syncdb",
            action="store_true",
            help="Creates tables for apps without migrations.",
        )
        parser.add_argument(
            "--check",
            action="store_true",
            dest="check_unapplied",
            help=(
                "Exits with a non-zero status if unapplied migrations exist and does "
                "not actually apply migrations."
            ),
        )
        parser.add_argument(
            "--prune",
            action="store_true",
            dest="prune",
            help="Delete nonexistent migrations from the django_migrations table.",
        )

    @no_translations
    def handle(self, *args, **options):
        database = options["database"]
        if not options["skip_checks"]:
            self.check(databases=[database])

        self.verbosity = options["verbosity"]
        self.interactive = options["interactive"]

        # 判断是否有 management 子模块，如果有则导入
        # Import the 'management' module within each installed app, to register
        # dispatcher events.
        for app_config in apps.get_app_configs():
            if module_has_submodule(app_config.module, "management"):
                import_module(".management", app_config.name)

        # 获取 connection，默认使用 default
        # Get the database we're operating from
        connection = connections[database]

        # 执行不同数据库的 prepare 操作
        # Hook for backends needing any database preparation
        connection.prepare_database()

        # 创建一个 migration 执行器，里面一个 MigrationLoader 和 MigrationRecorder
        # Work out which apps have migrations and which do not
        executor = MigrationExecutor(connection, self.migration_progress_callback)

        # 使用 loader 检查迁移记录，用于检查数据库中的 migration 记录是否正常
        # Raise an error if any migrations are applied before their dependencies.
        executor.loader.check_consistent_history(connection)

        # 检测是否冲突（有多个叶子节点）
        # Before anything else, see if there's conflicting apps and drop out
        # hard if there are any
        conflicts = executor.loader.detect_conflicts()
        if conflicts:
            name_str = "; ".join(
                "%s in %s" % (", ".join(names), app) for app, names in conflicts.items()
            )
            raise CommandError(
                "Conflicting migrations detected; multiple leaf nodes in the "
                "migration graph: (%s).\nTo fix them run "
                "'python manage.py makemigrations --merge'" % name_str
            )

        # If they supplied command line arguments, work out what they mean.
        run_syncdb = options["run_syncdb"]
        target_app_labels_only = True

        # 校验 app 名称
        if options["app_label"]:
            # Validate app_label.
            app_label = options["app_label"]
            try:
                apps.get_app_config(app_label)
            except LookupError as err:
                raise CommandError(str(err))
            if run_syncdb:
                # 如果使用了 --run-syncdb 命令，那么当前应用不能有 migration 文件，即应用不应该在已迁移的应用集合中
                if app_label in executor.loader.migrated_apps:
                    raise CommandError(
                        "Can't use run_syncdb with app '%s' as it has migrations."
                        % app_label
                    )
            # 如果没有使用 --run-syncdb 命令，那么当前应用就应该在已迁移的应用集合中，否则就报错
            elif app_label not in executor.loader.migrated_apps:
                raise CommandError("App '%s' does not have migrations." % app_label)

        # 如果同时传入了 app 名称和 migration 文件的名称
        if options["app_label"] and options["migration_name"]:
            migration_name = options["migration_name"]
            if migration_name == "zero":
                targets = [(app_label, None)]
            else:
                try:
                    # 根据传入的 migration 名称，根据前缀获取对应的 migration 文件
                    migration = executor.loader.get_migration_by_prefix(
                        app_label, migration_name
                    )
                except AmbiguityError:
                    # 匹配到多个 migration 文件
                    raise CommandError(
                        "More than one migration matches '%s' in app '%s'. "
                        "Please be more specific." % (migration_name, app_label)
                    )
                except KeyError:
                    # 未匹配到 migration 文件
                    raise CommandError(
                        "Cannot find a migration matching '%s' from app '%s'."
                        % (migration_name, app_label)
                    )
                target = (app_label, migration.name)

                # 判断是否要进行替换
                # Partially applied squashed migrations are not included in the
                # graph, use the last replacement instead.
                if (
                    target not in executor.loader.graph.nodes
                    and target in executor.loader.replacements
                ):
                    incomplete_migration = executor.loader.replacements[target]
                    target = incomplete_migration.replaces[-1]
                targets = [target]
            target_app_labels_only = False

        # 只传入了 app 名称
        elif options["app_label"]:
            # 取出对应 app 下的所有叶子节点
            targets = [
                key for key in executor.loader.graph.leaf_nodes() if key[0] == app_label
            ]
        # 啥也没传入，只执行了 migrate
        else:
            # 取出所有 app 的叶子节点
            targets = executor.loader.graph.leaf_nodes()

        if options["prune"]:
            if not options["app_label"]:
                raise CommandError(
                    "Migrations can be pruned only when an app is specified."
                )
            if self.verbosity > 0:
                self.stdout.write("Pruning migrations:", self.style.MIGRATE_HEADING)
            to_prune = set(executor.loader.applied_migrations) - set(
                executor.loader.disk_migrations
            )
            squashed_migrations_with_deleted_replaced_migrations = [
                migration_key
                for migration_key, migration_obj in executor.loader.replacements.items()
                if any(replaced in to_prune for replaced in migration_obj.replaces)
            ]
            if squashed_migrations_with_deleted_replaced_migrations:
                self.stdout.write(
                    self.style.NOTICE(
                        "  Cannot use --prune because the following squashed "
                        "migrations have their 'replaces' attributes and may not "
                        "be recorded as applied:"
                    )
                )
                for migration in squashed_migrations_with_deleted_replaced_migrations:
                    app, name = migration
                    self.stdout.write(f"    {app}.{name}")
                self.stdout.write(
                    self.style.NOTICE(
                        "  Re-run 'manage.py migrate' if they are not marked as "
                        "applied, and remove 'replaces' attributes in their "
                        "Migration classes."
                    )
                )
            else:
                to_prune = sorted(
                    migration for migration in to_prune if migration[0] == app_label
                )
                if to_prune:
                    for migration in to_prune:
                        app, name = migration
                        if self.verbosity > 0:
                            self.stdout.write(
                                self.style.MIGRATE_LABEL(f"  Pruning {app}.{name}"),
                                ending="",
                            )
                        executor.recorder.record_unapplied(app, name)
                        if self.verbosity > 0:
                            self.stdout.write(self.style.SUCCESS(" OK"))
                elif self.verbosity > 0:
                    self.stdout.write("  No migrations to prune.")

        # 根据 targets 获取到迁移计划
        plan = executor.migration_plan(targets)

        # 如果传递了 --plan
        if options["plan"]:
            self.stdout.write("Planned operations:", self.style.MIGRATE_LABEL)
            # 加上了 --plan，但是没有迁移计划
            if not plan:
                self.stdout.write("  No planned migration operations.")
            else:
                # 加上了 --plan，有迁移计划
                for migration, backwards in plan:
                    self.stdout.write(str(migration), self.style.MIGRATE_HEADING)
                    # 遍历每个迁移文件里面的 operations
                    for operation in migration.operations:
                        # 返回一个字符串，用于描述 --plan 的迁移操作。
                        message, is_error = self.describe_operation(
                            operation, backwards
                        )
                        style = self.style.WARNING if is_error else None
                        self.stdout.write("    " + message, style)
                if options["check_unapplied"]:
                    sys.exit(1)
            return
        if options["check_unapplied"]:
            if plan:
                sys.exit(1)
            return
        if options["prune"]:
            return

        # At this point, ignore run_syncdb if there aren't any apps to sync.
        run_syncdb = options["run_syncdb"] and executor.loader.unmigrated_apps
        # Print some useful info
        if self.verbosity >= 1:
            self.stdout.write(self.style.MIGRATE_HEADING("Operations to perform:"))
            if run_syncdb:
                if options["app_label"]:
                    self.stdout.write(
                        self.style.MIGRATE_LABEL(
                            "  Synchronize unmigrated app: %s" % app_label
                        )
                    )
                else:
                    self.stdout.write(
                        self.style.MIGRATE_LABEL("  Synchronize unmigrated apps: ")
                        + (", ".join(sorted(executor.loader.unmigrated_apps)))
                    )
            if target_app_labels_only:
                self.stdout.write(
                    self.style.MIGRATE_LABEL("  Apply all migrations: ")
                    + (", ".join(sorted({a for a, n in targets})) or "(none)")
                )
            else:
                if targets[0][1] is None:
                    self.stdout.write(
                        self.style.MIGRATE_LABEL("  Unapply all migrations: ")
                        + str(targets[0][0])
                    )
                else:
                    self.stdout.write(
                        self.style.MIGRATE_LABEL("  Target specific migration: ")
                        + "%s, from %s" % (targets[0][1], targets[0][0])
                    )

        #  创建一个 project_state
        pre_migrate_state = executor._create_project_state(with_applied_migrations=True)
        pre_migrate_apps = pre_migrate_state.apps
        # 发送一个 migrate 信号
        emit_pre_migrate_signal(
            self.verbosity,
            self.interactive,
            connection.alias,
            stdout=self.stdout,
            apps=pre_migrate_apps,
            plan=plan,
        )

        # Run the syncdb phase.
        if run_syncdb:
            if self.verbosity >= 1:
                self.stdout.write(
                    self.style.MIGRATE_HEADING("Synchronizing apps without migrations:")
                )
            if options["app_label"]:
                self.sync_apps(connection, [app_label])
            else:
                self.sync_apps(connection, executor.loader.unmigrated_apps)

        # 执行迁移动作
        # Migrate!
        if self.verbosity >= 1:
            self.stdout.write(self.style.MIGRATE_HEADING("Running migrations:"))
        # 没有执行计划
        if not plan:
            if self.verbosity >= 1:
                self.stdout.write("  No migrations to apply.")
                # If there's changes that aren't in migrations yet, tell them
                # how to fix it.
                # 检查数据库
                autodetector = MigrationAutodetector(
                    executor.loader.project_state(),
                    ProjectState.from_apps(apps),
                )
                # changes 方法检查 model 和 migration 文件是否有变化，返回值是app名称和变化的 Migration 对象
                changes = autodetector.changes(graph=executor.loader.graph)
                # 没有迁移计划，但是发现model和有变更。那么下面就提示让你先执行 manage.py makemigrations 命令
                if changes:
                    self.stdout.write(
                        self.style.NOTICE(
                            "  Your models in app(s): %s have changes that are not "
                            "yet reflected in a migration, and so won't be "
                            "applied." % ", ".join(repr(app) for app in sorted(changes))
                        )
                    )
                    self.stdout.write(
                        self.style.NOTICE(
                            "  Run 'manage.py makemigrations' to make new "
                            "migrations, and then re-run 'manage.py migrate' to "
                            "apply them."
                        )
                    )
            fake = False
            fake_initial = False
        # 有执行计划
        else:
            fake = options["fake"]
            fake_initial = options["fake_initial"]

        # 执行真正的迁移
        post_migrate_state = executor.migrate(
            targets,  # 叶子节点
            plan=plan,  # 执行计划
            state=pre_migrate_state.clone(),  # 拷贝状态
            fake=fake,
            fake_initial=fake_initial,
        )
        # post_migrate signals have access to all models. Ensure that all models
        # are reloaded in case any are delayed.
        post_migrate_state.clear_delayed_apps_cache()
        post_migrate_apps = post_migrate_state.apps

        # Re-render models of real apps to include relationships now that
        # we've got a final state. This wouldn't be necessary if real apps
        # models were rendered with relationships in the first place.
        with post_migrate_apps.bulk_update():
            model_keys = []
            for model_state in post_migrate_apps.real_models:
                model_key = model_state.app_label, model_state.name_lower
                model_keys.append(model_key)
                post_migrate_apps.unregister_model(*model_key)
        post_migrate_apps.render_multiple(
            [ModelState.from_model(apps.get_model(*model)) for model in model_keys]
        )

        # Send the post_migrate signal, so individual apps can do whatever they need
        # to do at this point.
        # 发送迁移信号
        emit_post_migrate_signal(
            self.verbosity,
            self.interactive,
            connection.alias,
            stdout=self.stdout,
            apps=post_migrate_apps,
            plan=plan,
        )

    def migration_progress_callback(self, action, migration=None, fake=False):
        if self.verbosity >= 1:
            compute_time = self.verbosity > 1
            if action == "apply_start":
                if compute_time:
                    self.start = time.monotonic()
                self.stdout.write("  Applying %s..." % migration, ending="")
                self.stdout.flush()
            elif action == "apply_success":
                elapsed = (
                    " (%.3fs)" % (time.monotonic() - self.start) if compute_time else ""
                )
                if fake:
                    self.stdout.write(self.style.SUCCESS(" FAKED" + elapsed))
                else:
                    self.stdout.write(self.style.SUCCESS(" OK" + elapsed))
            elif action == "unapply_start":
                if compute_time:
                    self.start = time.monotonic()
                self.stdout.write("  Unapplying %s..." % migration, ending="")
                self.stdout.flush()
            elif action == "unapply_success":
                elapsed = (
                    " (%.3fs)" % (time.monotonic() - self.start) if compute_time else ""
                )
                if fake:
                    self.stdout.write(self.style.SUCCESS(" FAKED" + elapsed))
                else:
                    self.stdout.write(self.style.SUCCESS(" OK" + elapsed))
            elif action == "render_start":
                if compute_time:
                    self.start = time.monotonic()
                self.stdout.write("  Rendering model states...", ending="")
                self.stdout.flush()
            elif action == "render_success":
                elapsed = (
                    " (%.3fs)" % (time.monotonic() - self.start) if compute_time else ""
                )
                self.stdout.write(self.style.SUCCESS(" DONE" + elapsed))

    def sync_apps(self, connection, app_labels):
        """Run the old syncdb-style operation on a list of app_labels."""
        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)

        # Build the manifest of apps and models that are to be synchronized.
        all_models = [
            (
                app_config.label,
                router.get_migratable_models(
                    app_config, connection.alias, include_auto_created=False
                ),
            )
            for app_config in apps.get_app_configs()
            if app_config.models_module is not None and app_config.label in app_labels
        ]

        def model_installed(model):
            opts = model._meta
            converter = connection.introspection.identifier_converter
            return not (
                (converter(opts.db_table) in tables)
                or (
                    opts.auto_created
                    and converter(opts.auto_created._meta.db_table) in tables
                )
            )

        manifest = {
            app_name: list(filter(model_installed, model_list))
            for app_name, model_list in all_models
        }

        # Create the tables for each model
        if self.verbosity >= 1:
            self.stdout.write("  Creating tables...")
        with connection.schema_editor() as editor:
            for app_name, model_list in manifest.items():
                for model in model_list:
                    # Never install unmanaged models, etc.
                    if not model._meta.can_migrate(connection):
                        continue
                    if self.verbosity >= 3:
                        self.stdout.write(
                            "    Processing %s.%s model"
                            % (app_name, model._meta.object_name)
                        )
                    if self.verbosity >= 1:
                        self.stdout.write(
                            "    Creating table %s" % model._meta.db_table
                        )
                    editor.create_model(model)

            # Deferred SQL is executed when exiting the editor's context.
            if self.verbosity >= 1:
                self.stdout.write("    Running deferred SQL...")

    # 返回一个字符串，用于描述 --plan 的迁移操作。
    @staticmethod
    def describe_operation(operation, backwards):
        """Return a string that describes a migration operation for --plan."""
        prefix = ""
        is_error = False
        if hasattr(operation, "code"):
            code = operation.reverse_code if backwards else operation.code
            action = (code.__doc__ or "") if code else None
        elif hasattr(operation, "sql"):
            action = operation.reverse_sql if backwards else operation.sql
        else:
            action = ""
            if backwards:
                prefix = "Undo "
        if action is not None:
            action = str(action).replace("\n", "")
        elif backwards:
            action = "IRREVERSIBLE"
            is_error = True
        if action:
            action = " -> " + action
        truncated = Truncator(action)
        return prefix + operation.describe() + truncated.chars(40), is_error
